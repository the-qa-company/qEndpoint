package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceFactory;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.Mutable;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.DecimalCompactString;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;

public class DecimalDictionarySectionMap implements DictionarySectionPrivate {
	protected FileChannel ch;
	protected Sequence blocks;
	BigMappedByteBuffer data;
	int blocksize;
	protected long numstrings;
	protected long size;

	private final File f;
	private final long startOffset;
	private final long endOffset;

	public DecimalDictionarySectionMap(CountInputStream input, File f) throws IOException {
		this.f = f;
		startOffset = input.getTotalBytes();

		CRCInputStream crcin = new CRCInputStream(input, new CRC8());

		// Read type
		int type = crcin.read();
		if (type != DecimalDictionarySection.TYPE_INDEX) {
			throw new IllegalFormatException(
					"Trying to read a DictionarySectionPFC from data that is not of the suitable type");
		}

		// Read vars
		numstrings = VByte.decode(crcin);
		size = VByte.decode(crcin);
		blocksize = (int) VByte.decode(crcin);

		if (!crcin.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}

		// Read blocks
		blocks = SequenceFactory.createStream(input, f);

		long base = input.getTotalBytes();
		IOUtil.skip(crcin, size + 4); // Including CRC32

		endOffset = input.getTotalBytes();

		// Read packed data
		ch = FileChannel.open(Paths.get(f.toString()));
		data = BigMappedByteBuffer.ofFileChannel(f.getAbsolutePath(), ch, FileChannel.MapMode.READ_ONLY, base, size);
	}

	/**
	 * Locate the block of a string doing binary search.
	 */
	protected long locateBlock(BigDecimal val) {
		long low = 0;
		long high = blocks.getNumberOfElements() - 1;
		long max = high;

		while (low <= high) {
			long mid = (low + high) >>> 1;

			int cmp;
			if (mid == max) {
				cmp = -1;
			} else {
				cmp = val.compareTo(extractDecimal(mid * blocksize + 1));
			}

			if (cmp < 0) {
				high = mid - 1;
			} else if (cmp != 0) {
				low = mid + 1;
			} else {
				return mid; // key found
			}
		}
		return -(low + 1); // key not found.
	}

	protected long locateInBlock(long blockid, BigDecimal val) {
		long blockStart = blocks.get(blockid);
		Mutable<Long> value = new Mutable<>(0L);
		long offset = blockStart + VByte.decode(data, blockStart, value);
		byte[] tmpBuffer = new byte[value.getValue().intValue()];

		long idInBlock = 0;

		// Read the first decimal in the block
		while (idInBlock < blocksize && offset < size) {
			// Decode scale
			offset += VByte.decodeSigned(data, offset, value);
			int scale = value.getValue().intValue();

			// Decode bigint
			offset += VByte.decode(data, offset, value);
			int bufferLen = value.getValue().intValue();
			data.get(tmpBuffer, offset, 0, bufferLen);
			offset += bufferLen;

			BigDecimal bd = new BigDecimal(new BigInteger(tmpBuffer, 0, bufferLen), scale);

			int cmp = val.compareTo(bd);

			if (cmp == 0) {
				return idInBlock; // what we are searching for
			}

			if (cmp < 0) {
				return 0; // after what we are searching for, we can stop
			}

			idInBlock++;
		}

		return 0;
	}

	@Override
	public long locate(CharSequence s) {
		ByteString bs = ByteString.of(s);

		BigDecimal val = bs.decimalValue();

		long blocknum = locateBlock(val);

		if (blocknum >= 0) {
			// Located exactly
			return (blocknum * blocksize) + 1;
		} else {
			// Not located exactly.
			blocknum = -blocknum - 2;

			if (blocknum >= 0) {
				long idblock = locateInBlock(blocknum, val);

				if (idblock != 0) {
					return (blocknum * blocksize) + idblock + 1;
				}
			}
		}

		return 0;
	}

	private BigDecimal extractDecimal(long id) {
		if (id < 1 || id > numstrings) {
			return BigDecimal.ZERO;
		}

		// Locate block
		long blockid = (id - 1) / blocksize;
		long nstring = (id - 1) % blocksize;

		long blockStart = blocks.get(blockid);
		Mutable<Long> value = new Mutable<>(0L);
		long offset = blockStart + VByte.decode(data, blockStart, value);
		byte[] tmpBuffer = new byte[value.getValue().intValue()];

		for (; ; ) {
			// Decode scale
			offset += VByte.decodeSigned(data, offset, value);
			int scale = value.getValue().intValue();

			// Decode bigint
			offset += VByte.decode(data, offset, value);
			int bufferLen = value.getValue().intValue();

			if (nstring == 0) {
				data.get(tmpBuffer, offset, 0, bufferLen);
				return new BigDecimal(new BigInteger(tmpBuffer, 0, bufferLen), scale);
			}

			offset += bufferLen;
			nstring--;
		}
	}

	@Override
	public CharSequence extract(long id) {
		if (id < 1 || id > numstrings) {
			return null;
		}

		return new DecimalCompactString(extractDecimal(id));
	}

	@Override
	public long size() {
		return size + blocks.size();
	}

	@Override
	public long getNumberOfElements() {
		return numstrings;
	}

	@Override
	public Iterator<? extends CharSequence> getSortedEntries() {
		return new Iterator<>() {
			long pos;

			@Override
			public boolean hasNext() {
				return pos < getNumberOfElements();
			}

			@Override
			public CharSequence next() {
				return extract(++pos);
			}
		};
	}

	@Override
	public void load(TempDictionarySection other, ProgressListener listener) {
		Iterator<? extends CharSequence> it = other.getSortedEntries();
		this.load(it, other.getNumberOfElements(), listener);
	}

	@Override
	public void load(Iterator<? extends CharSequence> it, long numentries, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		InputStream in = new BufferedInputStream(new FileInputStream(f));
		IOUtil.skip(in, startOffset);
		IOUtil.copyStream(in, output, endOffset - startOffset);
		in.close();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		try {
			Closer.closeAll(blocks, data, ch);
		} finally {
			blocks = null;
			data = null;
			ch = null;
		}
	}
}

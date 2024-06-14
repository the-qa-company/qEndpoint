package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceFactory;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.disk.BigBufferLongArray;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.IntCompactString;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;

public class IntDictionarySectionMap implements DictionarySectionPrivate {
	public static final int BLOCK_FLAGS_BITS_MASK = 63;
	public static final int BLOCK_FLAGS_SIGNED_FLAG = 64;
	public static final int BLOCK_FLAGS_SIZE = 7; // (64 - 1) max(6) + signed bit
	protected FileChannel ch;

	private final File f;
	private final long startOffset;
	private final long endOffset;

	protected Sequence blocks;
	LongArray data;
	int blocksize;
	protected long numstrings;
	protected long size;

	public IntDictionarySectionMap(CountInputStream input, File f) throws IOException {
		this.f = f;
		startOffset = input.getTotalBytes();

		CRCInputStream crcin = new CRCInputStream(input, new CRC8());

		// Read type
		int type = crcin.read();
		if (type != IntDictionarySection.TYPE_INDEX) {
			throw new IllegalFormatException(
					"Trying to read a DictionarySectionPFC from data that is not of the suitable type");
		}

		// Read vars
		numstrings = VByte.decode(crcin);
		this.size = VByte.decode(crcin);
		blocksize = (int) VByte.decode(crcin);

		if (!crcin.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}

		// Read blocks
		blocks = SequenceFactory.createStream(input, f);

		long base = input.getTotalBytes();
		IOUtil.skip(crcin, size * 8 + 4); // Including CRC32

		endOffset = input.getTotalBytes();

		// Read packed data
		ch = FileChannel.open(Paths.get(f.toString()));
		data = BigBufferLongArray.of(BigMappedByteBuffer.ofFileChannel(f.getAbsolutePath(), ch, FileChannel.MapMode.READ_ONLY, base, size * 8));
	}

	public long getBlockFlags(long blockStart) {
		return SequenceLog64BigDisk.getField(data, BLOCK_FLAGS_SIZE, 0, blockStart << 3);
	}

	/**
	 * Locate the block of a string doing binary search.
	 */
	protected long locateBlock(long val) {
		long low = 0;
		long high = blocks.getNumberOfElements() - 1;
		long max = high;

		while (low <= high) {
			long mid = (low + high) >>> 1;

			int cmp;
			if (mid == max) {
				cmp = -1;
			} else {
				long blockStart = extractLong(mid * blocksize + 1);
				cmp = Long.compare(val, blockStart);
			}

			if (cmp < 0) {
				high = mid - 1;
			} else if (cmp > 0) {
				low = mid + 1;
			} else {
				return mid; // key found
			}
		}
		return -(low + 1); // key not found.
	}

	protected long locateInBlock(long blockid, long val) {
		long blockStart = blocks.get(blockid);
		long flags = getBlockFlags(blockStart);
		int bits = (int) (flags & BLOCK_FLAGS_BITS_MASK) + 1;
		boolean signed = (flags & BLOCK_FLAGS_SIGNED_FLAG) != 0;

		long start = 0;
		long end;

		if (blockid == blocks.getNumberOfElements() - 2) {
			end = (numstrings - 1) % blocksize;
		} else {
			end = blocksize - 1;
		}
		long startOffset = (blockStart << 3) + BLOCK_FLAGS_SIZE;

		while (start <= end) {
			long mid = (start + end) >>> 1;

			long value = SequenceLog64BigDisk.getField(data, bits, mid, startOffset);
			if (signed) {
				if ((value & (1L << (bits - 1))) != 0 && bits != 64) {
					// signed and under 64, we need to negate the number
					value = (~0L << bits) | value;
				}
			}

			int cmp = Long.compare(val, value);

			if (cmp < 0) {
				end = mid - 1;
			} else if (cmp > 0) {
				start = mid + 1;
			} else {
				return mid;
			}
		}

		return 0;
	}

	@Override
	public long locate(CharSequence s) {
		ByteString bs = ByteString.of(s);

		long val = bs.longValue();

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

	private long extractLong(long id) {
		if (id < 1 || id > numstrings) {
			return 0;
		}

		// Locate block
		long blockid = (id - 1) / blocksize;
		long nstring = (id - 1) % blocksize;

		// we fetch the start of the block
		long blockStart = blocks.get(blockid);
		long flags = getBlockFlags(blockStart);
		int bits = (int) (flags & BLOCK_FLAGS_BITS_MASK) + 1;

		long value = SequenceLog64BigDisk.getField(data, bits, nstring, (blockStart << 3) + BLOCK_FLAGS_SIZE);

		if ((flags & BLOCK_FLAGS_SIGNED_FLAG) != 0) { // signed flag
			if ((value & (1L << (bits - 1))) != 0 && bits != 64) {
				// signed and under 64, we need to negate the number
				value = (~0L << bits) | value;
			}
		}
		return value;
	}

	@Override
	public CharSequence extract(long id) {
		if (id < 1 || id > numstrings) {
			return null;
		}

		// Locate block
		long blockid = (id - 1) / blocksize;
		long nstring = (id - 1) % blocksize;

		// we fetch the start of the block
		long blockStart = blocks.get(blockid);
		long flags = getBlockFlags(blockStart);
		int bits = (int) (flags & BLOCK_FLAGS_BITS_MASK) + 1;

		long value = SequenceLog64BigDisk.getField(data, bits, nstring, (blockStart << 3) + BLOCK_FLAGS_SIZE);

		if ((flags & BLOCK_FLAGS_SIGNED_FLAG) != 0) { // signed flag
			if ((value & (1L << (bits - 1))) != 0 && bits != 64) {
				// signed and under 64, we need to negate the number
				value = (~0L << bits) | value;
			}
		}
		return new IntCompactString(value);
	}

	@Override
	public long size() {
		return data.length() * data.sizeOf() + blocks.size();
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
				// FIXME: It is more efficient to go through each block, each
				// entry.
				pos++;
				return extract(pos);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public void load(TempDictionarySection other, ProgressListener listener) {
		throw new NotImplementedException();
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
		Closer.closeAll(blocks, data);
	}
}

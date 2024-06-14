package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64Big;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.Mutable;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.BigByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.CountOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.DecimalCompactString;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class DecimalDictionarySection implements DictionarySectionPrivate {
	public static final int NUMBER_PER_BLOCK = 64;
	public static final int TYPE_INDEX = 5;

	protected SequenceLog64Big blocks = new SequenceLog64Big();
	BigByteBuffer data = BigByteBuffer.allocate(0);
	int blocksize;
	protected long numstrings;
	protected long size;

	public DecimalDictionarySection(HDTOptions spec) {
		blocksize = (int) spec.getInt("rpl.decsec.blocksize", NUMBER_PER_BLOCK);
		if (blocksize <= 0) {
			throw new IllegalArgumentException("rpl.decsec.blocksize should be at least 1");
		}
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
		while (idInBlock < blocksize && offset < data.size()) {
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

		for (;;) {
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
		this.blocks = new SequenceLog64Big(64, (numentries - 1) / blocksize + 1);
		this.numstrings = numentries;

		Path file;

		try {
			file = File.createTempFile("hdt-rpl-ids", ".bin").toPath();
			long blockStart = 0;
			try {
				try (CountOutputStream os = new CountOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
					byte[][] buffer = new byte[blocksize][];
					int[] bufferScales = new int[blocksize];

					while (it.hasNext()) {
						// read all the strings
						int allocated = 0;
						int maxSize = 0;
						do {
							BigDecimal dec = ByteString.of(it.next()).decimalValue();

							bufferScales[allocated] = dec.scale();
							byte[] d = dec.unscaledValue().toByteArray();
							buffer[allocated++] = d;
							if (maxSize < d.length) maxSize = d.length;
						} while (allocated < blocksize && it.hasNext());
						blocks.append(blockStart);

						VByte.encode(os, maxSize); // max buffer size
						for (int i = 0; i < allocated; i++) {
							VByte.encodeSigned(os, bufferScales[i]); // scale
							IOUtil.writeSizedBuffer(os, buffer[i], listener); // unscaled value
						}

						blockStart = os.getTotalBytes();
					}
				}
				// end
				blocks.append(blockStart);
				size = blockStart;

				blocks.aggressiveTrimToSize();

				try (InputStream is = new BufferedInputStream(Files.newInputStream(file))) {
					data = BigByteBuffer.allocate(size);
					data.readStream(is, 0, size, listener);
				}

				Files.delete(file);
			} catch (Throwable t) {
				try {
					Files.deleteIfExists(file);
				} catch (IOException e) {
					t.addSuppressed(e);
				}
				throw t;
			}

		} catch (IOException e) {
			throw new RuntimeException("Error creating temporary file.", e);
		}
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		out.write(TYPE_INDEX);
		VByte.encode(out, numstrings);
		VByte.encode(out, data.size());
		VByte.encode(out, blocksize);

		out.writeCRC();

		blocks.save(output, listener);

		out.setCRC(new CRC32());
		data.writeStream(out, 0, data.size(), listener);
		out.writeCRC();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		CRCInputStream in = new CRCInputStream(input, new CRC8());

		// Read type
		int type = in.read();
		if (type != TYPE_INDEX) {
			throw new IllegalFormatException(
					"Trying to read a IntDictionarySection from data that is not of the suitable type");
		}

		numstrings = VByte.decode(in);
		this.size = VByte.decode(in);
		blocksize = (int) VByte.decode(in);

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}

		// Load block pointers
		blocks = new SequenceLog64Big();
		blocks.load(input, listener);

		in.setCRC(new CRC32());
		data = BigByteBuffer.allocate(size);
		data.readStream(in, 0, size, listener);

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section data.");
		}
	}

	@Override
	public void close() throws IOException {
		Closer.closeAll(blocks, data);
	}
}

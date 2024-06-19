package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64Big;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.unsafe.UnsafeLongArray;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.IntCompactString;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class IntDictionarySection implements DictionarySectionPrivate {
	public static final int BLOCK_FLAGS_BITS_MASK = 63;
	public static final int BLOCK_FLAGS_SIGNED_FLAG = 64;
	public static final int BLOCK_FLAGS_SIZE = 7; // (64 - 1) max(6) + signed
													// bit
	public static final int INT_PER_BLOCK = 1024;
	public static final int TYPE_INDEX = 3;

	protected SequenceLog64Big blocks = new SequenceLog64Big();
	UnsafeLongArray data;
	int blocksize;
	protected long numstrings;
	protected long size;

	public IntDictionarySection(HDTOptions spec) {
		blocksize = (int) spec.getInt("rpl.intsec.blocksize", INT_PER_BLOCK);
		if (blocksize <= 0) {
			throw new IllegalArgumentException("rpl.intsec.blocksize should be at least 1");
		}
	}

	public long getBlockFlags(long blockStart) {
		return SequenceLog64Big.getField(data, BLOCK_FLAGS_SIZE, 0, blockStart << 3);
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

			long value = SequenceLog64Big.getField(data, bits, mid, startOffset);
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

		long value = SequenceLog64Big.getField(data, bits, nstring, (blockStart << 3) + BLOCK_FLAGS_SIZE);

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

		long value = SequenceLog64Big.getField(data, bits, nstring, (blockStart << 3) + BLOCK_FLAGS_SIZE);

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
		this.blocks = new SequenceLog64Big(BitUtil.log2(other.size()), other.getNumberOfElements() / blocksize);
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
			long size = 0;
			long blockStart = 0;
			try {
				try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(file))) {
					UnsafeLongArray unalignedBuffer = UnsafeLongArray.wrapper(new long[2]);
					long[] buffer = new long[blocksize];

					while (it.hasNext()) {
						// read all the strings
						int allocated = 0;
						long min = Long.MAX_VALUE;
						long max = Long.MIN_VALUE;
						do {
							long val = ByteString.of(it.next()).longValue();
							buffer[allocated++] = val;
							if (min > val)
								min = val;
							if (max < val)
								max = val;
						} while (allocated < blocksize && it.hasNext());

						int flags = 0;
						int bits;
						if (min < 0) {
							flags |= BLOCK_FLAGS_SIGNED_FLAG;
							if (max < 0) {
								// we need to add 1 for the sign
								bits = BitUtil.log2(~min) + 1;
							} else {
								bits = Math.max(BitUtil.log2(~min), BitUtil.log2(max)) + 1;
							}
						} else {
							bits = BitUtil.log2(max);
						}

						if (bits == 0) {
							bits = 1; // wtf?
						}

						long mask = ~((~0L >>> bits) << bits);

						flags |= bits - 1;

						blocks.append(blockStart);

						long bitsRequired = BLOCK_FLAGS_SIZE + ((long) bits * allocated);
						long alignedSize = ((bitsRequired - 1) / 64 + 1) << 3;

						blockStart += alignedSize;

						// write the block flags
						SequenceLog64Big.setField(unalignedBuffer, 7, 0, flags, 0);

						int currentDelta = 7;
						// write the allocated fields

						for (int i = 0; i < allocated; i++) {
							SequenceLog64Big.setField(unalignedBuffer, bits, 0, buffer[i] & mask, currentDelta);
							currentDelta += bits;

							if (currentDelta >= 64) {
								size++;
								IOUtil.writeLong(os, unalignedBuffer.get(0));
								currentDelta -= 64;
								// swap the value
								unalignedBuffer.set(0, unalignedBuffer.get(1));
							}
						}
						if (currentDelta > 0) {
							size++;
							IOUtil.writeLong(os, unalignedBuffer.get(0));
						}
					}
				}
				// end
				blocks.append(blockStart);

				blocks.aggressiveTrimToSize();

				try (InputStream is = new BufferedInputStream(Files.newInputStream(file))) {
					data = UnsafeLongArray.allocate(size);
					data.read(is, size, 0);
					this.size = size;
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
		VByte.encode(out, data.length());
		VByte.encode(out, blocksize);

		out.writeCRC();

		blocks.save(output, listener);

		out.setCRC(new CRC32());
		data.write(out, size, 0);
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
		data = UnsafeLongArray.allocate(size);
		data.read(in, size, 0);

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section data.");
		}
	}

	@Override
	public void close() throws IOException {
		Closer.closeAll(blocks, data);
	}
}

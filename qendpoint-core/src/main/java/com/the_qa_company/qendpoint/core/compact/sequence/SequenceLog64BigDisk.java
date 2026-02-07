/**
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Dennis
 * Diefenbach: dennis.diefenbach@univ-st-etienne.fr
 */
package com.the_qa_company.qendpoint.core.compact.sequence;

import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.disk.LongArrayDisk;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;

public class SequenceLog64BigDisk implements DynamicSequence, Closeable {
	private static final byte W = 64;
	private static final int INDEX = 1073741824;
	private static final long[] BIT_MASK = new long[65];

	static {
		BIT_MASK[0] = 0L;
		for (int b = 1; b < 64; b++) {
			BIT_MASK[b] = (1L << b) - 1L;
		}
		BIT_MASK[64] = -1L;
	}

	LongArray data;
	private int numbits;
	private long numentries;
	private long maxvalue;

	public SequenceLog64BigDisk(String location) {
		this(location, W);
	}

	public SequenceLog64BigDisk(String location, int numbits) {
		this(location, numbits, 0);
	}

	public SequenceLog64BigDisk(String location, int numbits, long capacity) {
		this(location, numbits, capacity, false);
	}

	public SequenceLog64BigDisk(String location, int numbits, long capacity, boolean initialize) {
		this(Path.of(location), numbits, capacity, initialize);
	}

	public SequenceLog64BigDisk(Path location) {
		this(location, W);
	}

	public SequenceLog64BigDisk(Path location, int numbits) {
		this(location, numbits, 0);
	}

	public SequenceLog64BigDisk(Path location, int numbits, long capacity) {
		this(location, numbits, capacity, false);
	}

	public SequenceLog64BigDisk(Path location, int numbits, long capacity, boolean initialize) {
		this(location, numbits, capacity, initialize, true);
	}

	public SequenceLog64BigDisk(Path location, int numbits, long capacity, boolean initialize, boolean overwrite) {
		this.numentries = 0;
		this.numbits = numbits;
		this.maxvalue = BitUtil.maxVal(numbits);
		long size = numWordsFor(numbits, capacity);
		data = new LongArrayDisk(location, Math.max(size, 1), overwrite);
		if (initialize) {
			numentries = capacity;
		}
	}

	/** longs required to represent "total" integers of "bitsField" bits each */
	public static long numWordsFor(int bitsField, long total) {
		return ((bitsField * total + 63) / 64);
	}

	/** Number of bits required for last word */
	public static long lastWordNumBits(int bitsField, long total) {
		long totalBits = bitsField * total;
		if (totalBits == 0) {
			return 0;
		}
		return (totalBits - 1) % W + 1; // +1 To have output in the range 1-64,
										// -1 to compensate.
	}

	/** Number of bits required for last word */
	public static long lastWordNumBytes(int bitsField, long total) {
		return ((lastWordNumBits(bitsField, total) - 1) / 8) + 1; // +1 To have
																	// output in
																	// the range
																	// 1-8, -1
																	// to
																	// compensate.
	}

	/** Number of bytes required to represent n integers of e bits each */
	public static long numBytesFor(int bitsField, long total) {
		return (bitsField * total + 7) / 8;
	}

	/**
	 * Retrieve a given index from array data where every value uses bitsField
	 * bits
	 *
	 * @param data      Array
	 * @param bitsField Length in bits of each field
	 * @param index     Position to be retrieved
	 */
	private static long getField(LongArray data, int bitsField, long index) {
		if (bitsField == 0)
			return 0;

		long bitPos = index * bitsField;
		long i = bitPos / W;
		long j = bitPos % W;
		long fieldMask = BIT_MASK[bitsField];
		long result;
		if (j + bitsField <= W) {
			result = (data.get(i) >>> j) & fieldMask;
		} else {
			result = data.get(i) >>> j;
			result = result | (data.get(i + 1) << ((W << 1) - j - bitsField)) >>> (W - bitsField);
		}
		return result;
	}

	/**
	 * Store a given value in index into array data where every value uses
	 * bitsField bits
	 *
	 * @param data      Array
	 * @param bitsField Length in bits of each field
	 * @param index     Position to store in
	 * @param value     Value to be stored
	 */
	private static void setField(LongArray data, int bitsField, long index, long value) {
		if (bitsField == 0)
			return;

		final long fieldMask = BIT_MASK[bitsField];
		final long v = value & fieldMask;
		final long bitPos = index * (long) bitsField;
		final long wordIndex = bitPos >> 6;
		final int bitOffset = (int) (bitPos & 63L);

		final long w0 = data.get(wordIndex);
		final int endBit = bitOffset + bitsField;
		if (endBit <= W) {
			final long maskAtOffset = fieldMask << bitOffset;
			data.set(wordIndex, (w0 & ~maskAtOffset) | (v << bitOffset));
			return;
		}

		final int bitsInFirst = W - bitOffset;
		final int bitsInSecond = endBit - W;
		final long maskFirst = BIT_MASK[bitsInFirst];
		final long maskSecond = BIT_MASK[bitsInSecond];
		final long newW0 = (w0 & ~(maskFirst << bitOffset)) | ((v & maskFirst) << bitOffset);
		data.set(wordIndex, newW0);

		final long wordIndex1 = wordIndex + 1;
		final long w1 = data.get(wordIndex1);
		final long newW1 = (w1 & ~maskSecond) | ((v >>> bitsInFirst) & maskSecond);
		data.set(wordIndex1, newW1);
	}

	private void resizeArray(long size) throws IOException {
		data.resize(size);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#add(java.util.Iterator)
	 */
	@Override
	public void add(Iterator<Long> elements) {
	}

	public void addIntegers(ArrayList<Integer> elements) {
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#get(long)
	 */
	@Override
	public long get(long position) {
//		if(position<0 || position>=numentries) {
		// System.out.println("pos, numentries:"+position+","+numentries);
		// throw new IndexOutOfBoundsException();
//		}

		if (position < 0 || numWordsFor(numbits, position) > data.length()) {
			throw new IndexOutOfBoundsException(
					position + " < 0 || " + position + " > " + data.length() * 64 / numbits);
		}

		return getField(data, numbits, position);
	}

	@Override
	public void set(long position, long value) {
		if (value < 0 || value > maxvalue) {
			throw new IllegalArgumentException(
					"Value exceeds the maximum for this data structure " + value + " > " + maxvalue);
		}

		// System.out.println("numbits "+this.numbits);
		setField(data, numbits, position, value);
	}

	public void set(long[] positions, long[] values, int offset, int length) {
		if (length <= 0) {
			return;
		}
		if (positions == null || values == null) {
			throw new NullPointerException();
		}
		if (offset < 0 || length < 0 || offset + length > positions.length || offset + length > values.length) {
			throw new IndexOutOfBoundsException();
		}

		if (numbits == 0) {
			return;
		}
		if (numbits >= 64 || !(data instanceof LongArrayDisk disk)) {
			for (int i = offset; i < offset + length; i++) {
				set(positions[i], values[i]);
			}
			return;
		}

		sortPairsByKey(positions, values, offset, offset + length);

		final long fieldMask = BIT_MASK[numbits];
		long[] run = getBulkWriteRunBuffer(length);
		long runStart = -1;
		int runLength = 0;
		long runLastIndex = -1;

		long currentWordIndex = -1;
		long currentWordValue = 0;
		boolean currentDirty = false;

		long nextWordIndex = -1;
		long nextWordValue = 0;
		boolean nextDirty = false;

		for (int i = offset; i < offset + length; i++) {
			long position = positions[i];
			long value = values[i];

			if (value < 0 || value > maxvalue) {
				throw new IllegalArgumentException(
						"Value exceeds the maximum for this data structure " + value + " > " + maxvalue);
			}

			long bitPos = position * (long) numbits;
			long wordIndex = bitPos >>> 6;
			int bitOffset = (int) (bitPos & 63);

			if (wordIndex == nextWordIndex) {
				if (currentDirty) {
					if (runLength == 0) {
						runStart = currentWordIndex;
						runLastIndex = currentWordIndex;
						run[0] = currentWordValue;
						runLength = 1;
					} else if (currentWordIndex == runLastIndex + 1) {
						if (runLength == run.length) {
							disk.set(runStart, run, 0, runLength);
							runStart = currentWordIndex;
							runLastIndex = currentWordIndex;
							run[0] = currentWordValue;
							runLength = 1;
						} else {
							run[runLength++] = currentWordValue;
							runLastIndex = currentWordIndex;
						}
					} else {
						disk.set(runStart, run, 0, runLength);
						runStart = currentWordIndex;
						runLastIndex = currentWordIndex;
						run[0] = currentWordValue;
						runLength = 1;
					}
				}
				currentWordIndex = nextWordIndex;
				currentWordValue = nextWordValue;
				currentDirty = nextDirty;
				nextWordIndex = -1;
				nextDirty = false;
			} else if (wordIndex != currentWordIndex) {
				if (currentDirty) {
					if (runLength == 0) {
						runStart = currentWordIndex;
						runLastIndex = currentWordIndex;
						run[0] = currentWordValue;
						runLength = 1;
					} else if (currentWordIndex == runLastIndex + 1) {
						if (runLength == run.length) {
							disk.set(runStart, run, 0, runLength);
							runStart = currentWordIndex;
							runLastIndex = currentWordIndex;
							run[0] = currentWordValue;
							runLength = 1;
						} else {
							run[runLength++] = currentWordValue;
							runLastIndex = currentWordIndex;
						}
					} else {
						disk.set(runStart, run, 0, runLength);
						runStart = currentWordIndex;
						runLastIndex = currentWordIndex;
						run[0] = currentWordValue;
						runLength = 1;
					}
				}
				if (nextDirty) {
					if (runLength == 0) {
						runStart = nextWordIndex;
						runLastIndex = nextWordIndex;
						run[0] = nextWordValue;
						runLength = 1;
					} else if (nextWordIndex == runLastIndex + 1) {
						if (runLength == run.length) {
							disk.set(runStart, run, 0, runLength);
							runStart = nextWordIndex;
							runLastIndex = nextWordIndex;
							run[0] = nextWordValue;
							runLength = 1;
						} else {
							run[runLength++] = nextWordValue;
							runLastIndex = nextWordIndex;
						}
					} else {
						disk.set(runStart, run, 0, runLength);
						runStart = nextWordIndex;
						runLastIndex = nextWordIndex;
						run[0] = nextWordValue;
						runLength = 1;
					}
					nextWordIndex = -1;
					nextDirty = false;
				}
				currentWordIndex = wordIndex;
				currentWordValue = disk.get(wordIndex);
				currentDirty = false;
				nextWordIndex = -1;
				nextDirty = false;
			}

			final int endBit = bitOffset + numbits;
			long mask = fieldMask << bitOffset;
			currentWordValue = (currentWordValue & ~mask) | (value << bitOffset);
			currentDirty = true;

			if (endBit > W) {
				long spillIndex = wordIndex + 1;
				if (nextWordIndex != spillIndex) {
					if (nextDirty) {
						if (runLength == 0) {
							runStart = nextWordIndex;
							runLastIndex = nextWordIndex;
							run[0] = nextWordValue;
							runLength = 1;
						} else if (nextWordIndex == runLastIndex + 1) {
							if (runLength == run.length) {
								disk.set(runStart, run, 0, runLength);
								runStart = nextWordIndex;
								runLastIndex = nextWordIndex;
								run[0] = nextWordValue;
								runLength = 1;
							} else {
								run[runLength++] = nextWordValue;
								runLastIndex = nextWordIndex;
							}
						} else {
							disk.set(runStart, run, 0, runLength);
							runStart = nextWordIndex;
							runLastIndex = nextWordIndex;
							run[0] = nextWordValue;
							runLength = 1;
						}
					}
					nextWordIndex = spillIndex;
					nextWordValue = disk.get(spillIndex);
					nextDirty = false;
				}

				long nextMask = ~0L << (numbits + bitOffset - W);
				nextWordValue = (nextWordValue & nextMask) | (value >>> (W - bitOffset));
				nextDirty = true;
			}
		}

		if (currentDirty) {
			if (runLength == 0) {
				runStart = currentWordIndex;
				runLastIndex = currentWordIndex;
				run[0] = currentWordValue;
				runLength = 1;
			} else if (currentWordIndex == runLastIndex + 1) {
				if (runLength == run.length) {
					disk.set(runStart, run, 0, runLength);
					runStart = currentWordIndex;
					runLastIndex = currentWordIndex;
					run[0] = currentWordValue;
					runLength = 1;
				} else {
					run[runLength++] = currentWordValue;
					runLastIndex = currentWordIndex;
				}
			} else {
				disk.set(runStart, run, 0, runLength);
				runStart = currentWordIndex;
				runLastIndex = currentWordIndex;
				run[0] = currentWordValue;
				runLength = 1;
			}
		}
		if (nextDirty) {
			if (runLength == 0) {
				runStart = nextWordIndex;
				runLastIndex = nextWordIndex;
				run[0] = nextWordValue;
				runLength = 1;
			} else if (nextWordIndex == runLastIndex + 1) {
				if (runLength == run.length) {
					disk.set(runStart, run, 0, runLength);
					runStart = nextWordIndex;
					runLastIndex = nextWordIndex;
					run[0] = nextWordValue;
					runLength = 1;
				} else {
					run[runLength++] = nextWordValue;
					runLastIndex = nextWordIndex;
				}
			} else {
				disk.set(runStart, run, 0, runLength);
				runStart = nextWordIndex;
				runLastIndex = nextWordIndex;
				run[0] = nextWordValue;
				runLength = 1;
			}
		}

		if (runLength > 0) {
			disk.set(runStart, run, 0, runLength);
		}
	}

	private static final ThreadLocal<long[]> BULK_WRITE_RUN_BUFFER = ThreadLocal.withInitial(() -> new long[4096]);

	private static long[] getBulkWriteRunBuffer(int fieldCount) {
		int required = Math.max(16, fieldCount * 2 + 2);
		long[] buffer = BULK_WRITE_RUN_BUFFER.get();
		if (buffer.length >= required) {
			return buffer;
		}

		int newSize = 1;
		while (newSize < required) {
			newSize <<= 1;
		}
		buffer = new long[newSize];
		BULK_WRITE_RUN_BUFFER.set(buffer);
		return buffer;
	}

	private static void sortPairsByKey(long[] keys, long[] values, int from, int to) {
		int left = from;
		int right = to - 1;
		if (left >= right) {
			return;
		}

		long pivot = keys[left + ((right - left) >>> 1)];
		int i = left;
		int j = right;
		while (i <= j) {
			while (keys[i] < pivot) {
				i++;
			}
			while (keys[j] > pivot) {
				j--;
			}
			if (i <= j) {
				long tmpKey = keys[i];
				keys[i] = keys[j];
				keys[j] = tmpKey;

				long tmpVal = values[i];
				values[i] = values[j];
				values[j] = tmpVal;

				i++;
				j--;
			}
		}
		if (from < j + 1) {
			sortPairsByKey(keys, values, from, j + 1);
		}
		if (i < to) {
			sortPairsByKey(keys, values, i, to);
		}
	}

	@Override
	public int sizeOf() {
		return numbits;
	}

	@Override
	public void append(long value) {

		// assert numentries<Integer.MAX_VALUE;

		// if(value<0 || value>maxvalue) {
		// throw new IllegalArgumentException("Value exceeds the maximum for
		// this data structure");
		// }

		long neededSize = numWordsFor(numbits, numentries + 1);
		if (data.length() < neededSize) {
			try {
				resizeArray(data.length() * 2);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		this.set(numentries, value);
		numentries++;
	}

	@Override
	public void aggressiveTrimToSize() {
		long max = 0;
		// Count and calculate number of bits needed per element.
		for (long i = 0; i < numentries; i++) {
			long value = this.get(i);
			if (value > max) {
				max = value;
			}
		}
		int newbits = BitUtil.log2(max);

		assert newbits <= numbits;
		// System.out.println("newbits"+newbits);
		if (newbits != numbits) {
			for (long i = 0; i < numentries; i++) {
				long value = getField(data, numbits, i);
				setField(data, newbits, i, value);
			}
			numbits = newbits;
			maxvalue = BitUtil.maxVal(numbits);

			long totalSize = numWordsFor(numbits, numentries);

			if (totalSize != data.length()) {
				try {
					resizeArray(totalSize);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

	}

	@Override
	public void trimToSize() {
		try {
			resizeArray(numWordsFor(numbits, numentries));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void resize(long numentries) {
		this.numentries = numentries;
		try {
			resizeArray(numWordsFor(numbits, numentries));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void clear() {
		data.clear();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#getNumberOfElements()
	 */
	@Override
	public long getNumberOfElements() {
		return numentries;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#save(java.io.OutputStream,
	 * hdt.ProgressListener)
	 */
	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());
		// write type of blocks
		out.write(SequenceFactory.TYPE_SEQLOG);
		// write the number of bits
		out.write(numbits);
		VByte.encode(out, numentries);
		// write CRC
		out.writeCRC();

		out.setCRC(new CRC32());

		long numwords = numWordsFor(numbits, numentries);
		for (long i = 0; i < numwords - 1; i++) {
			IOUtil.writeLong(out, data.get(i));
		}

		if (numwords > 0) {
			// Write only used bits from last entry (byte aligned, little
			// endian)
			long lastWordUsedBits = lastWordNumBits(numbits, numentries);
			BitUtil.writeLowerBitsByteAligned(data.get(numwords - 1), lastWordUsedBits, out);
		}

		out.writeCRC();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) {
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#size()
	 */
	@Override
	public long size() {
		return numBytesFor(numbits, numentries);
	}

	public long getRealSize() {
		return data.length() * 8L;
	}

	public int getNumBits() {
		return numbits;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.compact.array.Stream#getType()
	 */
	@Override
	public String getType() {
		return HDTVocabulary.SEQ_TYPE_LOG;
	}

	@Override
	public void close() throws IOException {
		try {
			IOUtil.closeObject(data);
		} finally {
			data = null;
		}
	}
}

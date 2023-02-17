/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact
 * /sequence/SequenceLog64.java $ Revision: $Rev: 130 $ Last modified: $Date:
 * 2013-01-21 00:09:42 +0000 (lun, 21 ene 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.compact.sequence;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author mario.arias
 */
public class SequenceLog64 implements DynamicSequence {
	protected static final byte W = 64;

	protected long[] data;
	protected int numbits;
	protected long numentries;
	protected long maxvalue;

	public SequenceLog64() {
		this(W);
	}

	public SequenceLog64(int numbits) {
		this(numbits, 0);
	}

	public SequenceLog64(int numbits, long capacity) {
		this.numentries = 0;
		this.numbits = numbits;
		this.maxvalue = BitUtil.maxVal(numbits);

		long size = numWordsFor(numbits, capacity);
		assert size >= 0;

		data = new long[Math.max((int) size, 1)];
	}

	public SequenceLog64(int numbits, long capacity, boolean initialize) {
		this(numbits, capacity);
		if (initialize) {
			numentries = capacity;
		}
	}

	/** longs required to represent "total" integers of "bitsField" bits each */
	public static long numWordsFor(int bitsField, long total) {
		return (bitsField * total + 63) / 64;
	}

	/** Number of bits required for last word */
	public static int lastWordNumBits(int bitsField, long total) {
		long totalBits = bitsField * total;
		if (totalBits == 0) {
			return 0;
		}
		return (int) ((totalBits - 1) % W) + 1; // +1 To have output in the
												// range 1-64, -1 to compensate.
	}

	/** Number of bits required for last word */
	public static int lastWordNumBytes(int bitsField, long total) {
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
	private static long getField(long[] data, int bitsField, long index) {
		if (bitsField == 0)
			return 0;

		long bitPos = index * bitsField;
		int i = (int) (bitPos / W);
		int j = (int) (bitPos % W);
		long result;
		if (j + bitsField <= W) {
			result = (data[i] << (W - j - bitsField)) >>> (W - bitsField);
		} else {
			result = data[i] >>> j;
			result = result | (data[i + 1] << ((W << 1) - j - bitsField)) >>> (W - bitsField);
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
	private static void setField(long[] data, int bitsField, long index, long value) {
		if (bitsField == 0)
			return;

		long bitPos = index * bitsField;
		int i = (int) (bitPos / W);
		int j = (int) (bitPos % W);

		long mask = ~(~0L << bitsField) << j;
		data[i] = (data[i] & ~mask) | (value << j);

		if (j + bitsField > W) {
			mask = ~0L << (bitsField + j - W);
			data[i + 1] = (data[i + 1] & mask) | value >>> (W - j);
		}
	}

	private void resizeArray(int size) {
		data = Arrays.copyOf(data, size);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#add(java.util.Iterator)
	 */
	@Override
	public void add(Iterator<Long> elements) {
		long max = 0;
		numentries = 0;

		// Count and calculate number of bits needed per element.
		while (elements.hasNext()) {
			long val = elements.next();
			max = val > max ? val : max;
			numentries++;
		}

		// Prepare array
		numbits = BitUtil.log2(max);
		int size = (int) numWordsFor(numbits, numentries);
		data = new long[size];

		// Save
		int count = 0;
		while (elements.hasNext()) {
			long element = elements.next();
			assert element <= maxvalue;
			setField(data, numbits, count, element);
			count++;
		}
	}

	public void addIntegers(List<Integer> elements) {
		long max = 0;
		numentries = 0;

		// Count and calculate number of bits needed per element.
		for (int i = 0; i < elements.size(); i++) {
			long val = elements.get(i).longValue();
			max = val > max ? val : max;
			numentries++;
		}

		// Prepare array
		numbits = BitUtil.log2(max);
		int size = (int) numWordsFor(numbits, numentries);
		data = new long[size];

		// Save
		int count = 0;
		for (int i = 0; i < elements.size(); i++) {
			long element = elements.get(i).longValue();
			assert element <= maxvalue;
			setField(data, numbits, count, element);
			count++;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#get(int)
	 */
	@Override
	public long get(long position) {
		if (position < 0 || position >= numentries) {
			throw new IndexOutOfBoundsException();
		}

		return getField(data, numbits, position);
	}

	@Override
	public void set(long position, long value) {
		if (value < 0 || value > maxvalue) {
			throw new IllegalArgumentException("Value exceeds the maximum for this data structure");
		}
		setField(data, numbits, position, value);
	}

	@Override
	public int sizeOf() {
		return numbits;
	}

	@Override
	public void append(long value) {
		if (value < 0 || value > maxvalue) {
			throw new IllegalArgumentException("Value exceeds the maximum for this data structure");
		}

		long neededSize = numWordsFor(numbits, numentries + 1);
		if (neededSize > Integer.MAX_VALUE - 5) {
			throw new IllegalArgumentException(
					"Needed size exceeds the maximum size of this data structure " + neededSize);
		}
		if (data.length < neededSize) {
			resizeArray((int) Math.min(Integer.MAX_VALUE - 5L, data.length * 2L));
		}

		this.set((int) numentries, value);
		numentries++;
	}

	@Override
	public void aggressiveTrimToSize() {
		long max = 0;
		// Count and calculate number of bits needed per element.
		for (int i = 0; i < numentries; i++) {
			long value = this.get(i);
			max = value > max ? value : max;
		}
		int newbits = BitUtil.log2(max);

		assert newbits <= numbits;

		if (newbits != numbits) {
			for (int i = 0; i < numentries; i++) {
				long value = getField(data, numbits, i);
				setField(data, newbits, i, value);
			}
			numbits = newbits;
			maxvalue = BitUtil.maxVal(numbits);
			long totalSize = numWordsFor(numbits, numentries);
			resizeArray((int) totalSize);
		}

	}

	@Override
	public void trimToSize() {
		resizeArray((int) numWordsFor(numbits, numentries));
	}

	@Override
	public void resize(long numentries) {
		this.numentries = numentries;
		resizeArray((int) numWordsFor(numbits, numentries));
	}

	@Override
	public void clear() {
		Arrays.fill(data, 0);
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

		out.write(SequenceFactory.TYPE_SEQLOG);
		out.write(numbits);
		VByte.encode(out, numentries);

		out.writeCRC();

		out.setCRC(new CRC32());

		int numwords = (int) numWordsFor(numbits, numentries);
		for (int i = 0; i < numwords - 1; i++) {
			IOUtil.writeLong(out, data[i]);
		}

		if (numwords > 0) {
			// Write only used bits from last entry (byte aligned, little
			// endian)
			int lastWordUsedBits = lastWordNumBits(numbits, numentries);
			BitUtil.writeLowerBitsByteAligned(data[numwords - 1], lastWordUsedBits, out);
		}

		out.writeCRC();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#load(java.io.InputStream,
	 * hdt.ProgressListener)
	 */
	@SuppressWarnings("resource")
	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		CRCInputStream in = new CRCInputStream(input, new CRC8());

		int type = in.read();
		if (type != SequenceFactory.TYPE_SEQLOG) {
			throw new IllegalFormatException("Trying to read a LogArray but the data is not LogArray");
		}
		numbits = in.read();
		numentries = VByte.decode(in);

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading LogArray64 header.");
		}

		if (numbits > 64) {
			throw new IllegalFormatException("LogArray64 cannot deal with more than 64bit per entry");
		}

		in.setCRC(new CRC32());

		int numwords = (int) numWordsFor(numbits, numentries);
		data = new long[numwords];
		for (int i = 0; i < numwords - 1; i++) {
			data[i] = IOUtil.readLong(in);
		}

		if (numwords > 0) {
			// Read only used bits from last entry (byte aligned, little endian)
			int lastWordUsed = lastWordNumBits(numbits, numentries);
			data[numwords - 1] = BitUtil.readLowerBitsByteAligned(lastWordUsed, in);
		}

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading LogArray64 data.");
		}
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
		return data.length * 8L;
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

	public long binSearchExact(long element, long begin, long end) throws NotFoundException {
		while (begin <= end) {
			long mid = (begin + end) / 2;
			long read = get(mid);
			if (element > read) {
				begin = mid + 1;
			} else if (element < read) {
				end = mid - 1;
			} else {
				return mid;
			}
		}
		throw new NotFoundException();
	}

	public long binSearch(long element, long fromIndex, long toIndex) {
		long low = fromIndex;
		long high = toIndex - 1;

		while (low <= high) {
			long mid = (low + high) >>> 1;
			long midVal = get(mid);

			if (midVal < element)
				low = mid + 1;
			else if (midVal > element)
				high = mid - 1;
			else
				return mid; // key found
		}
		return -(low + 1); // key not found.
	}

	@Override
	public void close() throws IOException {
		data = null;
	}
}

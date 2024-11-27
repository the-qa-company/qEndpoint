package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * A class to buffer write to a long array to chunk the sets and sort them by
 * index before calling them
 *
 * @author Antoine Willerval
 */
public class WriteLongArrayBuffer implements LongArray, Closeable {
	// debug field
	private static final boolean DISABLE_BUFFER = true;
	private final LongArray array;
	private ArrayElementLong[] bufferLong;
	private ArrayElementInt[] bufferInt;
	private int index = 0;
	private boolean lastOrder;
	private final long[] prevFound = new long[16384];

	/**
	 * create the buffer
	 *
	 * @param array      the array to write
	 * @param maxValue   the maximum value, to use int64 or int32
	 * @param maxElement count of long elements to store
	 */
	public WriteLongArrayBuffer(LongArray array, long maxValue, int maxElement) {
		this.array = array;
		if (!DISABLE_BUFFER) {
			int bits = BitUtil.log2(maxValue + 2) + CompressUtil.INDEX_SHIFT; // +
																				// 1
																				// for
																				// shared

			if (bits > 31) {
				bufferLong = new ArrayElementLong[maxElement / 3];
			} else {
				// we can store twice as many elements, so we add * 2L
				bufferInt = new ArrayElementInt[(int) (maxElement / 3)];
			}
		}
	}

	/**
	 * clear all the elements
	 */
	@Override
	public void clear() {
		index = 0;
	}

	@Override
	public long[] getPrevFound() {
		return prevFound;
	}

	public void free() {
		flush();
		bufferInt = null;
		bufferLong = null;
		System.gc();
	}

	private ArrayElement get(int index) {
		if (bufferLong != null) {
			return bufferLong[index];
		} else if (bufferInt != null) {
			return bufferInt[index];
		} else {
			throw new IllegalArgumentException("free buffer!");
		}
	}

	private void checkConsistency() {
		if (size() == maxCapacity()) {
			flush();
		}
	}

	/**
	 * write all the sets and clear the buffer
	 */
	public void flush() {
		// ignore empty array
		if (size() == 0) {
			return;
		}

		// sort the set calls
		if (bufferLong != null) {
			Arrays.sort(bufferLong, 0, size(), ArrayElement::compareTo);
		} else if (bufferInt != null) {
			Arrays.sort(bufferInt, 0, size(), ArrayElement::compareTo);
		} else {
			return;
		}

		// reverse the order to write from the end to the start
		if (lastOrder) {
			for (int i = 0; i < index; i++) {
				ArrayElement e = get(i);
				array.set(e.getIndex(), e.getValue());
			}
		} else {
			for (int i = index - 1; i >= 0; i--) {
				ArrayElement e = get(i);
				array.set(e.getIndex(), e.getValue());
			}
		}
		// reverse for next run
		lastOrder = !lastOrder;
		// clear the buffer
		clear();
	}

	/**
	 * get a value of the buffer, will flush all remaining sets before
	 *
	 * @param index the index
	 * @return the value
	 */
	@Override
	public long get(long index) {
		flush();
		return array.get(index);
	}

	/**
	 * set a value in the array
	 *
	 * @param index the index
	 * @param value the value to set
	 */
	@Override
	public void set(long index, long value) {
		if (DISABLE_BUFFER) {
			array.set(index, value);
			return;
		}
		if (bufferLong != null) {
			bufferLong[this.index++] = new ArrayElementLong(index, value);
		} else {
			bufferInt[this.index++] = new ArrayElementInt(index, value);
		}
		// check for flush
		checkConsistency();
	}

	/**
	 * get the length of the array, will flush remaining sets before
	 *
	 * @return the length of the array
	 */
	@Override
	public long length() {
		flush();
		return array.length();
	}

	@Override
	public int sizeOf() {
		return array.sizeOf();
	}

	@Override
	public void resize(long newSize) throws IOException {
		array.resize(newSize);
	}

	/**
	 * @return the used size of the buffer
	 */
	public int size() {
		return index;
	}

	/**
	 * @return the max capacity of the buffer
	 */
	public int maxCapacity() {
		if (bufferLong != null) {
			return bufferLong.length;
		} else {
			return bufferInt.length;
		}
	}

	@Override
	public void close() throws IOException {
		flush();
		if (array instanceof Closeable) {
			((Closeable) array).close();
		}
	}

	private interface ArrayElement extends Comparable<ArrayElement> {
		long getIndex();

		long getValue();

		@Override
		default int compareTo(ArrayElement o) {
			return Long.compare(getIndex(), o.getIndex());
		}
	}

	private static class ArrayElementLong implements ArrayElement {
		private final long index, value;

		public ArrayElementLong(long index, long value) {
			this.index = index;
			this.value = value;
		}

		@Override
		public long getIndex() {
			return index;
		}

		@Override
		public long getValue() {
			return value;
		}
	}

	private static class ArrayElementInt implements ArrayElement {
		private final int index, value;

		public ArrayElementInt(long index, long value) {
			this.index = (int) index;
			this.value = (int) value;
		}

		@Override
		public long getIndex() {
			return index;
		}

		@Override
		public long getValue() {
			return value;
		}
	}
}

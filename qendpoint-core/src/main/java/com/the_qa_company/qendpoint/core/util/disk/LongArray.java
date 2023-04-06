package com.the_qa_company.qendpoint.core.util.disk;

import java.io.IOException;
import java.util.stream.LongStream;

/**
 * Describe a large array of longs
 */
public interface LongArray {
	/**
	 * get an element at a particular index
	 *
	 * @param index the index
	 * @return the value
	 */
	long get(long index);

	/**
	 * Set a new value at the specified position.
	 *
	 * @param index the index
	 * @param value the value
	 */
	void set(long index, long value);

	/**
	 * @return the length of the array
	 */
	long length();

	/**
	 * @return if the array is empty
	 */
	default boolean isEmpty() {
		return length() == 0;
	}

	/**
	 * @return size of the components (in bits)
	 */
	int sizeOf();

	/**
	 * resize the array
	 *
	 * @param newSize new size
	 * @throws IOException io exception
	 */
	void resize(long newSize) throws IOException;

	/**
	 * clear the long array, ie: set 0s
	 */
	void clear();

	/**
	 * @return sync version of this long array, might return this if this
	 * LongArray is already a sync array
	 */
	default LongArray asSync() {
		return SyncLongArray.of(this);
	}

	/**
	 * run a binary search over this array, the array should be sorted!
	 *
	 * @param value the value to search
	 * @return index of the value, -1 if it doesn't appear in the array
	 * @see #linearSearch(long)
	 */
	default long binarySearch(long value) {
		long min = 0;
		long max = length();

		while (min < max) {
			long mid = (min + max) / 2;

			long mappedValue = get(mid);
			if (mappedValue == value) {
				return mid;
			} else if (mappedValue < value) {
				min = mid + 1;
			} else { // mappedValue > value
				max = mid;
			}
		}

		return -1;
	}

	/**
	 * run a linear search over this array
	 *
	 * @param value the value to search
	 * @return index of the value, -1 if it doesn't appear in the array
	 * @see #binarySearch(long)
	 */
	default long linearSearch(long value) {
		for (int i = 0; i < length(); i++) {
			if (get(i) == value) {
				return i;
			}
		}

		return -1;
	}

	/**
	 * @return stream of the array
	 */
	default LongStream stream() {
		return LongStream.range(0, length()).map(this::get);
	}
}

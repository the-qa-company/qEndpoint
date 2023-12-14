package com.the_qa_company.qendpoint.core.util.disk;

import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.LongStream;

public interface ReadOnlyLongArray extends Iterable<Long> {
	/**
	 * get an element at a particular index
	 *
	 * @param index the index
	 * @return the value
	 */
	long get(long index);

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
	 * run a binary search over this array, the array should be sorted!
	 *
	 * @param value the value to search
	 * @return index of the value, -1 if it doesn't appear in the array
	 * @see #linearSearch(long)
	 */
	default long binarySearch(long value) {
		return binarySearch(value, 0, length());
	}

	/**
	 * run a binary search over this array, the array should be sorted!
	 *
	 * @param value      the value to search
	 * @param startIndex start index (inclusive)
	 * @param endIndex   end index (exclusive)
	 * @return index of the value, -1 if it doesn't appear in the array
	 * @see #linearSearch(long)
	 */
	default long binarySearch(long value, long startIndex, long endIndex) {
		long min = startIndex;
		long max = endIndex;

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
	 * run a binary search over this array, the array should be sorted!
	 *
	 * @param value the value to search
	 * @return index of the value, or the index it would be if it was in this
	 *         array
	 * @see #linearSearch(long)
	 */
	default long binarySearchLocation(long value) {
		return binarySearchLocation(value, 0, length());
	}

	/**
	 * run a binary search over this array, the array should be sorted!
	 *
	 * @param value      the value to search
	 * @param startIndex start index (inclusive)
	 * @param endIndex   end index (exclusive)
	 * @return index of the value, or the index it would be if it was in this
	 *         array
	 * @see #linearSearch(long)
	 */
	default long binarySearchLocation(long value, long startIndex, long endIndex) {
		long min = startIndex;
		long max = endIndex;

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
		return min;
	}

	/**
	 * run a linear search over this array
	 *
	 * @param value the value to search
	 * @return index of the value, -1 if it doesn't appear in the array
	 * @see #binarySearch(long)
	 */
	default long linearSearch(long value) {
		return linearSearch(value, 0, length());
	}

	/**
	 * run a linear search over this array
	 *
	 * @param value      the value to search
	 * @param startIndex start index (inclusive)
	 * @param endIndex   end index (exclusive)
	 * @return index of the value, -1 if it doesn't appear in the array
	 * @see #binarySearch(long)
	 */
	default long linearSearch(long value, long startIndex, long endIndex) {
		for (long i = startIndex; i < endIndex; i++) {
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

	@Override
	default Iterator<Long> iterator() {
		return new Iterator<>() {
			long index;

			@Override
			public boolean hasNext() {
				return index < length();
			}

			@Override
			public Long next() {
				return get(index++);
			}
		};
	}
}

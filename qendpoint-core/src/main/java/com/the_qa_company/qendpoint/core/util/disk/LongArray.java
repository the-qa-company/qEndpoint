package com.the_qa_company.qendpoint.core.util.disk;

import java.io.IOException;

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
	 *         LongArray is already a sync array
	 */
	default LongArray asSync() {
		return SyncLongArray.of(this);
	}
}

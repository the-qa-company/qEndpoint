package com.the_qa_company.qendpoint.core.util.disk;

import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.LongStream;

/**
 * Describe a large array of longs
 */
public interface LongArray extends ReadOnlyLongArray {
	/**
	 * create an in memory long array
	 *
	 * @param size size
	 * @return long array
	 */
	static LongArray of(long size) {
		return new LargeLongArray(IOUtil.createLargeArray(size));
	}

	/**
	 * Set a new value at the specified position.
	 *
	 * @param index the index
	 * @param value the value
	 */
	void set(long index, long value);

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

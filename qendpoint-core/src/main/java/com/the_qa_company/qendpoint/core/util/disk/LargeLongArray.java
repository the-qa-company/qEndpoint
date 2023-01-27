package com.the_qa_company.qendpoint.core.util.disk;

import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.visnow.jlargearrays.LargeArrayUtils;
import org.visnow.jlargearrays.LongLargeArray;

import java.io.IOException;

/**
 * Implementation of {@link LongArray} using a LargeArray
 *
 * @author Antoine Willerval
 */
public class LargeLongArray implements LongArray {
	private LongLargeArray array;

	/**
	 * @param array large array
	 */
	public LargeLongArray(LongLargeArray array) {
		this.array = array;
	}

	@Override
	public long get(long index) {
		return array.getLong(index);
	}

	@Override
	public void set(long index, long value) {
		array.setLong(index, value);
	}

	@Override
	public long length() {
		return array.length();
	}

	@Override
	public int sizeOf() {
		return (int) array.getType().sizeOf() * 8;
	}

	@Override
	public void resize(long newSize) throws IOException {
		if (newSize > 0) {
			if (array.length() != newSize) {
				LongLargeArray a = IOUtil.createLargeArray(newSize, false);
				LargeArrayUtils.arraycopy(array, 0, a, 0, Math.min(newSize, array.length()));
				array = a;
			}
		}
	}

	@Override
	public void clear() {
		IOUtil.fillLargeArray(array, 0);
	}
}

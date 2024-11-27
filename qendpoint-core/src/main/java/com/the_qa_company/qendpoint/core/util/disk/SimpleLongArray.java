package com.the_qa_company.qendpoint.core.util.disk;

import java.io.IOException;
import java.util.Arrays;

public class SimpleLongArray extends AbstractLongArray {

	public static LongArray of(int size) {
		return wrapper(new long[size]);
	}

	public static LongArray wrapper(long[] array) {
		return new SimpleLongArray(array);
	}

	private SimpleLongArray(long[] array) {
		this.array = array;
	}

	private long[] array;

	@Override
	public long get(long index) {
		if (index < 0 || index >= array.length) {
			throw new IndexOutOfBoundsException(index + " < 0 || " + index + " > " + array.length);
		}
		return array[(int) index];
	}

	@Override
	protected void innerSet(long index, long value) {
		if (index < 0 || index >= array.length) {
			throw new IndexOutOfBoundsException(index + " < 0 || " + index + " > " + array.length);
		}
		array[(int) index] = value;
	}

	@Override
	public long length() {
		return array.length;
	}

	@Override
	public int sizeOf() {
		return 64;
	}

	@Override
	public void resize(long newSize) throws IOException {
		if (Integer.MAX_VALUE - 5 <= newSize) {
			throw new IllegalArgumentException("Can't rezise to a size bigger than " + (Integer.MAX_VALUE - 5));
		}
		long[] newArray = new long[(int) newSize];

		System.arraycopy(this.array, 0, newArray, 0, Math.min(this.array.length, newArray.length));
		this.array = newArray;
	}

	@Override
	public void clear() {
		Arrays.fill(array, 0);
	}

}

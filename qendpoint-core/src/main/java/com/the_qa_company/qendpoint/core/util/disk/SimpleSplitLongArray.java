package com.the_qa_company.qendpoint.core.util.disk;

import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Implementation of LongArray for simple int64 splits
 */
public class SimpleSplitLongArray implements LongArray, Closeable {
	final LongArray array;
	private final int shift;
	private final long max;
	private final int indexMask;
	private final int numbits;

	private long size;

	private SimpleSplitLongArray(LongArray array, int numbits, long size) {
		this.size = size;
		this.array = array;
		int b = Long.bitCount(numbits);
		if (b != 1 || numbits < 1 || numbits > 64) {
			throw new IllegalArgumentException("numbits should be 2, 4, 8, ... 64");
		}
		shift = 6 - BitUtil.log2(numbits - 1);
		max = (~0L) >>> (64 - numbits);
		indexMask = (1 << shift) - 1;
		this.numbits = numbits;
	}

	public static SimpleSplitLongArray int8Array(long size) {
		return intXArray(size, 8);
	}

	public static SimpleSplitLongArray int16Array(long size) {
		return intXArray(size, 16);
	}

	public static SimpleSplitLongArray int32Array(long size) {
		return intXArray(size, 32);
	}

	public static SimpleSplitLongArray int64Array(long size) {
		return intXArray(size, 64);
	}

	public static SimpleSplitLongArray intXArray(long size, int x) {
		return new SimpleSplitLongArray(new LargeLongArray(IOUtil.createLargeArray(1 + size / (64 / x))), x, size);
	}

	public static SimpleSplitLongArray int8ArrayDisk(Path location, long size) {
		return intXArrayDisk(location, size, 8);
	}

	public static SimpleSplitLongArray int16ArrayDisk(Path location, long size) {
		return intXArrayDisk(location, size, 16);
	}

	public static SimpleSplitLongArray int32ArrayDisk(Path location, long size) {
		return intXArrayDisk(location, size, 32);
	}

	public static SimpleSplitLongArray int64ArrayDisk(Path location, long size) {
		return intXArrayDisk(location, size, 64);
	}

	public static SimpleSplitLongArray intXArrayDisk(Path location, long size, int x) {
		return new SimpleSplitLongArray(new LongArrayDisk(location, 1 + size / (64 / x)), x, size);
	}

	@Override
	public long get(long index) {
		long rindex = index >>> shift;
		int sindex = (int) (index & indexMask) << (6 - shift);
		return max & (array.get(rindex) >>> sindex);
	}

	@Override
	public void set(long index, long value) {
		long rindex = index >>> shift;
		int sindex = (int) (index & indexMask) << (6 - shift);

		long old = array.get(rindex);
		long v = (old & ~(max << sindex)) | ((max & value) << sindex);

		if (old != v) {
			array.set(rindex, v);
		}
	}

	@Override
	public long length() {
		return size;
	}

	@Override
	public int sizeOf() {
		return 1 << (6 - shift);
	}

	@Override
	public void resize(long newSize) throws IOException {
		size = newSize;
		array.resize(newSize / (64 / numbits));
	}

	@Override
	public void clear() {
		array.clear();
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeObject(array);
	}
}

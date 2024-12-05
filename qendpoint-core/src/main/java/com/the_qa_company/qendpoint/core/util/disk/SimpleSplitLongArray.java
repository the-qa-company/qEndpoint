package com.the_qa_company.qendpoint.core.util.disk;

import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.apache.jena.base.Sys;

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

	private final int PREV_FOUND_SIZE = 1024 * 128;

	// should take about 1MB per array when PREV_FOUND_SIZE is 1024 * 128
	private final long[] prevFoundHigh = new long[PREV_FOUND_SIZE];
	private final long[] prevFoundLow = new long[PREV_FOUND_SIZE];
	private final long[] prevFoundMid = new long[PREV_FOUND_SIZE];

	private long size;
	private int prevFoundBucketSize;

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

	long actualMax = 0;

	@Override
	public int getPrevFoundBucketSize() {
		return prevFoundBucketSize;
	}

	private void updatePrevFoundBucketSize() {
		int div = (int) (actualMax / PREV_FOUND_SIZE) - 1;
		// we want to have the next power of 2
		int next = 1;
		while (next < div) {
			next <<= 1;
		}
		int min = Math.max(next, 1024);
		this.prevFoundBucketSize = min;
	}

	public long getEstimatedMidpoint(long val, long min, long max) {
		int index = (int) (val / getPrevFoundBucketSize() + 1);
		if (index >= prevFoundMid.length) {
			return (min + max) / 2;
		}
		long t = prevFoundMid[index];
		if (t > min && t < max) {
			return t;
		} else {
			return (min + max) / 2;
		}
	}

	@Override
	public void prevFoundMid(long val, long min) {
		int index = (int) (val / getPrevFoundBucketSize() + 1);
		if (index >= prevFoundMid.length) {
			return;
		}
		// print index, existing val and new val and the value of min
//		System.out.println("index: " + index + " existing val: " + prevFoundMid[index] + " new val: " + val + " min: " + min);

		prevFoundMid[index] = min;
	}

	@Override
	public long getLowerBound(long val) {
		int index = (int) (val / getPrevFoundBucketSize() + 1);
		if (index - 1 >= 0) {
			long t = prevFoundHigh[index - 1];
			if (t > 0) {
				return t;
			}
		}
		return 0;
	}

	@Override
	public long getUpperBound(long val) {
		int index = (int) (val / getPrevFoundBucketSize() + 1);

		var prevFound = prevFoundLow;

		if (index + 1 < prevFound.length) {
			long t = prevFound[index + 1];
			if (t > 0) {
				return Math.min(length(), t);
			}
		}

		return length();
	}

	@Override
	public void updatePrevFound() {
		updatePrevFoundBucketSize();
		int prevFoundBucketSize = getPrevFoundBucketSize();
		int i = 0;
		long len = length();
		while (i < len) {
			long val = get(i);
			if (val == 0) {
				i++;
				continue;
			}

			int index = (int) (val / prevFoundBucketSize + 1);
			prevFoundHigh[index] = Math.max(prevFoundHigh[index], i);
			if (prevFoundLow[index] == 0) {
				prevFoundLow[index] = i;
			} else {
				prevFoundLow[index] = Math.min(prevFoundLow[index], i);
			}
			prevFoundMid[index] = (prevFoundHigh[index] + prevFoundLow[index]) / 2;

			i++;
			if (i % (1024 * 128) == 0) {
				System.out.println("i: " + i);
			}
		}
		System.out.println();
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
		actualMax = Math.max(actualMax, value);

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
	public long[] getPrevFoundHigh() {
		return prevFoundHigh;
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeObject(array);
	}
}

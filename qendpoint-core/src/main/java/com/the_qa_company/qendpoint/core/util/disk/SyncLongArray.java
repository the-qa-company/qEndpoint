package com.the_qa_company.qendpoint.core.util.disk;

import java.io.IOException;

/**
 * sync a long array
 *
 * @author Antoine Willerval
 */
public class SyncLongArray implements LongArray {
	/**
	 * Sync a long array
	 *
	 * @param other the array
	 * @return sync version of the long array, might return the array if already
	 *         sync
	 */
	public static SyncLongArray of(LongArray other) {
		if (other instanceof SyncLongArray) {
			return (SyncLongArray) other;
		}
		return new SyncLongArray(other);
	}

	private final LongArray array;

	private SyncLongArray(LongArray array) {
		this.array = array;
	}

	@Override
	public synchronized long get(long index) {
		return array.get(index);
	}

	@Override
	public synchronized void set(long index, long value) {
		array.set(index, value);
	}

	@Override
	public synchronized long length() {
		return array.length();
	}

	@Override
	public synchronized int sizeOf() {
		return array.sizeOf();
	}

	@Override
	public synchronized void resize(long newSize) throws IOException {
		array.resize(newSize);
	}

	@Override
	public synchronized void clear() {
		array.clear();
	}
}

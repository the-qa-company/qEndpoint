package com.the_qa_company.qendpoint.core.util.disk;

import java.io.IOException;

public class LongArrayView implements LongArray {
	private final LongArray parent;
	private final long start;
	private final long length;

	public LongArrayView(LongArray parent, long start, long length) {
		this.parent = parent;

		if (length < 0) {
			throw new IllegalArgumentException("the length of a view can't be negative!");
		}
		if (length != 0) {
			if (start < 0) {
				throw new IllegalArgumentException("the start of a view can't be negative!");
			}

			if (start + length > parent.length()) {
				throw new IndexOutOfBoundsException("start + length > parent size");
			}
			this.start = start;
		} else {
			this.start = 0;
		}
		this.length = length;
	}

	@Override
	public long get(long index) {
		return parent.get(start + index);
	}

	@Override
	public void set(long index, long value) {
		parent.set(start + index, value);
	}

	@Override
	public long length() {
		return length;
	}

	@Override
	public int sizeOf() {
		return parent.sizeOf();
	}

	@Override
	public void resize(long newSize) throws IOException {
		throw new IllegalArgumentException("Can't resize a view");
	}

	@Override
	public void clear() {
		throw new IllegalArgumentException("Can't clear a view");
	}

	@Override
	public LongArray view(long start, long length) {
		if (parent instanceof LongArrayView lgv) {
			// we need to check this part here to be sure we're not writing
			// inside the parent
			if (start + length > parent.length()) {
				throw new IndexOutOfBoundsException("start + length > parent size");
			}
			return new LongArrayView(lgv.parent, lgv.start + start, length);
		}
		return LongArray.super.view(start, length);
	}
}

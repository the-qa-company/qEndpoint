package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.List;

/**
 * A {@link SizedSupplier} backed by a pre-buffered {@link List}. The size is a
 * fixed estimate for the full list, typically bytes.
 */
public final class ListSizedSupplier<E> implements SizedSupplier<E> {
	private final List<E> list;
	private final long size;
	private int idx;

	public ListSizedSupplier(List<E> list, long size) {
		this.list = list;
		this.size = size;
	}

	@Override
	public E get() {
		if (idx >= list.size()) {
			return null;
		}
		return list.get(idx++);
	}

	@Override
	public long getSize() {
		return size;
	}
}

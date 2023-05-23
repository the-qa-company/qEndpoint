package com.the_qa_company.qendpoint.core.storage.iterator;

public class EmptyCloseableIteration<T, E extends Exception> implements CloseableIterator<T, E> {
	@Override
	public void close() {
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public T next() {
		return null;
	}
}

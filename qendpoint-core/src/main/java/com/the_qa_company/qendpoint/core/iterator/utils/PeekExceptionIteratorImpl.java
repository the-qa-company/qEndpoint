package com.the_qa_company.qendpoint.core.iterator.utils;

public class PeekExceptionIteratorImpl<T, E extends Exception> implements PeekExceptionIterator<T, E> {
	private final ExceptionIterator<T, E> it;
	private T next;

	public PeekExceptionIteratorImpl(ExceptionIterator<T, E> it) {
		this.it = it;
	}

	@Override
	public boolean hasNext() throws E {
		if (next != null) {
			return true;
		}
		if (!it.hasNext()) {
			return false;
		}
		next = it.next();
		return true;
	}

	@Override
	public T next() throws E {
		try {
			return peek();
		} finally {
			next = null;
		}
	}

	@Override
	public T peek() throws E {
		if (hasNext()) {
			return next;
		}
		return null;
	}

	public ExceptionIterator<T, E> getWrappedIterator() {
		return it;
	}
}

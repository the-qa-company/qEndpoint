package com.the_qa_company.qendpoint.core.iterator.utils;

/**
 * {@link ExceptionIterator} version of {@link FetcherIterator}
 *
 * @param <T> iterator type
 * @param <E> exception type
 */
public abstract class FetcherExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {
	private T next;

	protected FetcherExceptionIterator() {
	}

	/**
	 * @return the next element, or null if it is the end
	 */
	protected abstract T getNext() throws E;

	@Override
	public boolean hasNext() throws E {
		if (next != null) {
			return true;
		}
		next = getNext();
		return next != null;
	}

	@Override
	public T next() throws E {
		try {
			return peek();
		} finally {
			next = null;
		}
	}

	/**
	 * @return peek the element without passing to the next element
	 */
	public T peek() throws E {
		if (hasNext()) {
			return next;
		}
		return null;
	}
}

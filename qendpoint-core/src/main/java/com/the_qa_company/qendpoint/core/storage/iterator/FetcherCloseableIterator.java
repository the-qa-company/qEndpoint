package com.the_qa_company.qendpoint.core.storage.iterator;

/**
 * Version of
 * {@link com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator} in
 * {@link CloseableIterator} context
 *
 * @author Antoine Willerval
 * @param <T> iterator type
 * @param <E> close type
 */
public abstract class FetcherCloseableIterator<T, E extends Exception> implements CloseableIterator<T> {
	private T next;

	protected FetcherCloseableIterator() {
	}

	/**
	 * @return the next element, or null if it is the end
	 */
	protected abstract T getNext();

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
		}
		next = getNext();
		return next != null;
	}

	@Override
	public T next() {
		try {
			return peek();
		} finally {
			next = null;
		}
	}

	/**
	 * @return peek the element without passing to the next element
	 */
	public T peek() {
		if (hasNext()) {
			return next;
		}
		return null;
	}
}

package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Iterator implementation without the next element fetching method
 *
 * @param <T> iterator type
 */
public abstract class FetcherIterator<T> implements Iterator<T> {
	private T next;

	protected FetcherIterator() {
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

	/**
	 * map this iterator
	 *
	 * @param mappingFunction func
	 * @param <M>             new type
	 * @return iterator
	 */
	public <M> Iterator<M> map(Function<T, M> mappingFunction) {
		return new MapIterator<>(this, mappingFunction);
	}
}

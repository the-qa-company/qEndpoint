package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Iterator with peek-able element
 *
 * @param <T> iterator type
 */
public class PeekIteratorImpl<T> implements PeekIterator<T> {
	private final Iterator<T> it;
	private T next;

	public PeekIteratorImpl(Iterator<T> it) {
		this.it = it;
	}

	@Override
	public boolean hasNext() {
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
	public T next() {
		try {
			return peek();
		} finally {
			next = null;
		}
	}

	@Override
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

	public Iterator<T> getWrappedIterator() {
		return it;
	}
}

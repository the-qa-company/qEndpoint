package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class PrefetchIterator<T> implements Iterator<T> {

	protected T next;
	private boolean used;

	@Override
	public boolean hasNext() {
		// If we call prefetch() from the constructor, the attributes of the
		// child class has not been initialized yet.
		// Therefore we delay the call to prefetch() to the first call to
		// hasNext()
		if (!used) {
			next = prefetch();
			used = true;
		}
		return next != null;
	}

	@Override
	public T next() {
		if (next == null) {
			throw new NoSuchElementException();
		}
		T returned = next;

		try {
			next = prefetch();
		} catch (NoSuchElementException e) {
			next = null;
		}

		return returned;
	}

	/**
	 * Reimplement this method to return an element on each call. Return null,
	 * or throw a {@link NoSuchElementException} Exception when no more elements
	 * available.
	 *
	 * @return The element, or null when finished.
	 */
	protected abstract T prefetch();

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}

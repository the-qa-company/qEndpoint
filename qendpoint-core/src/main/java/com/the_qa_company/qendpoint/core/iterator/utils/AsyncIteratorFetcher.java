package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Synchronise an iterator
 *
 * @param <E> iterator type
 * @author Antoine Willerval
 */
public class AsyncIteratorFetcher<E> implements Supplier<E> {
	private final Iterator<E> iterator;
	private final Lock lock = new ReentrantLock();
	private boolean end;

	public AsyncIteratorFetcher(Iterator<E> iterator) {
		this.iterator = iterator;
	}

	/**
	 * @return an element from the iterator, this method is thread safe
	 */
	@Override
	public E get() {
		lock.lock();
		try {
			if (iterator.hasNext()) {
				return iterator.next();
			}
			end = true;
			return null;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * @return is the end
	 */
	public boolean isEnd() {
		return end;
	}
}

package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
	private boolean end;

	private ConcurrentLinkedQueue<E> queue = new ConcurrentLinkedQueue<>();

	public AsyncIteratorFetcher(Iterator<E> iterator) {
		this.iterator = iterator;
	}

	/**
	 * @return an element from the iterator, this method is thread safe
	 */
//	@Override
//	public E get() {
//		lock.lock();
//		try {
//			if (iterator.hasNext()) {
//				return iterator.next();
//			}
//			end = true;
//			return null;
//		} finally {
//			lock.unlock();
//		}
//	}

	AtomicInteger counter = new AtomicInteger(0);

	@Override
	public E get() {
		E poll = queue.poll();
		if (poll != null) {
			counter.incrementAndGet();
			return poll;
		}

		synchronized (this) {
			poll = queue.poll();
			if (poll != null) {
				counter.incrementAndGet();
				return poll;
			}

			ConcurrentLinkedQueue<E> newqueue = new ConcurrentLinkedQueue<>();

			for (int i = 0; i < 128 && iterator.hasNext(); i++) {
				if (poll == null) {
					poll = iterator.next();
				}
				newqueue.add(iterator.next());
			}
			this.queue = newqueue;
			if (poll != null) {
				counter.incrementAndGet();
				return poll;
			}

			System.out.println("AsyncIteratorFetcher: " + counter.get());

			end = true;
			return null;
		}
	}

	/**
	 * @return is the end
	 */
	public boolean isEnd() {
		return end;
	}
}

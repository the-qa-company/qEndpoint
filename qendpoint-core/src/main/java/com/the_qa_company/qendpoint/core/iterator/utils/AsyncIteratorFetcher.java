package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Queue;
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
	public static final int BUFFER = 1024 * 4;
	private final Iterator<E> iterator;
	private final Lock lock = new ReentrantLock();
	private boolean end;
	Queue<E>[] queue = new Queue[] { new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER),
			new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER),
			new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER),
			new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER),
			new ArrayDeque(BUFFER), };

	public AsyncIteratorFetcher(Iterator<E> iterator) {
		this.iterator = iterator;
	}

	/**
	 * @return an element from the iterator, this method is thread safe
	 */
	@Override
	public E get() {

		int index = (int) (Thread.currentThread().getId() % queue.length);

		// With this approach there is some risk that a queue is filled but
		// never emptied. Maybe we should look for another queue to read from
		// before filling our own queue?
		synchronized (queue[index]) {
			E poll = queue[index].poll();

			if (poll != null) {
				return poll;
			}

			synchronized (this) {
				poll = queue[index].poll();
				if (poll == null) {
					if (iterator.hasNext()) {
						poll = iterator.next();
					}
					ArrayList<E> objects = new ArrayList<>(BUFFER);

					for (int i = 0; i < BUFFER && iterator.hasNext(); i++) {
						objects.add(iterator.next());
					}

					queue[index].addAll(objects);
				}

				if (poll == null) {
					end = true;
				}
				return poll;
			}
		}

	}

	/**
	 * @return is the end
	 */
	public boolean isEnd() {
		return end;
	}
}

package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Synchronise an iterator
 *
 * @param <E> iterator type
 * @author Antoine Willerval
 */
public class AsyncIteratorFetcherUnordered<E> extends AsyncIteratorFetcher<E> {
	public static final int BUFFER = 1024 * 4;
	private final Iterator<E> iterator;
	private final Lock lock = new ReentrantLock();
	private boolean end;
	volatile Queue<E>[] queue = new Queue[] { new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER),
			new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER),
			new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER),
			new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER), new ArrayDeque(BUFFER),
			new ArrayDeque(BUFFER), };

	public AsyncIteratorFetcherUnordered(Iterator<E> iterator) {
		super(iterator);
		this.iterator = iterator;
	}

	/**
	 * @return an element from the iterator, this method is thread safe
	 */
	@Override
	public E get() {

		int index = (int) (Thread.currentThread().getId() % queue.length);

		Queue<E> es = queue[index];
		if (es == null) {
			for (Queue<E> eQueue : queue) {
				if (eQueue != null) {
					synchronized (eQueue) {
						E poll = eQueue.poll();

						if (poll != null) {
							return poll;
						}
					}
				}
			}
		}

		if (es != null) {
			// With this approach there is some risk that a queue is filled but
			// never emptied. Maybe we should look for another queue to read
			// from
			// before filling our own queue?
			synchronized (es) {
				E poll = es.poll();

				if (poll != null) {
					return poll;
				}

				synchronized (this) {
					es = queue[index];
					if (es != null) {

						poll = es.poll();
						if (poll == null) {
							if (iterator.hasNext()) {
								poll = iterator.next();
								ArrayList<E> objects = new ArrayList<>(BUFFER);

								for (int i = 0; i < BUFFER && iterator.hasNext(); i++) {
									es.add(iterator.next());
								}

								es.addAll(objects);
							}

						}

						if (poll == null) {
							queue[index] = null;
						} else {
							return poll;
						}
					}
				}
			}
		}

		for (Queue<E> eQueue : queue) {
			if (eQueue != null) {

				synchronized (eQueue) {
					synchronized (this) {
						E poll = eQueue.poll();

						if (poll != null) {
							return poll;
						}
					}
				}
			}
		}
		end = true;
		return null;

	}

	/**
	 * @return is the end
	 */
	public boolean isEnd() {
		return end;
	}
}

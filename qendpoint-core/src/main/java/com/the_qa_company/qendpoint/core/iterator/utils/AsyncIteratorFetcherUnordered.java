package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/**
 * Synchronise an iterator
 *
 * @param <E> iterator type
 * @author Håvard M. Ottestad
 * @author Antoine Willerval
 */
public class AsyncIteratorFetcherUnordered<E> extends AsyncIteratorFetcher<E> {

	private static final int CORES = Runtime.getRuntime().availableProcessors();

	public static final int BUFFER;

	static {
		long maxMemory = Runtime.getRuntime().maxMemory() / 1024 / 1024; // in
																			// MB
		if (maxMemory >= 32 * 1024) {
			BUFFER = 1024 * 32;
		} else if (maxMemory >= 16 * 1024) {
			BUFFER = 1024 * 16;
		} else if (maxMemory >= 8 * 1024) {
			BUFFER = 1024 * 8;
		} else if (maxMemory >= 4 * 1024) {
			BUFFER = 1024 * 4;
		} else if (maxMemory >= 2 * 1024) {
			BUFFER = 1024 * 2;
		} else {
			BUFFER = 1024;
		}
	}

	private final Iterator<E> iterator;
	private boolean end;
	volatile Queue<E>[] queue = new Queue[CORES * 2];

	{
		for (int i = 0; i < queue.length; i++) {
			queue[i] = new ArrayDeque<>(BUFFER);
		}
	}

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
								for (int i = 0; i < BUFFER && iterator.hasNext(); i++) {
									es.add(iterator.next());
								}
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

		synchronized (this) {
			if (iterator.hasNext()) {
				E poll = iterator.next();
				return poll;
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

package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

/**
 * Iterator to split an iterator stream into multiple files, the iterator return
 * {@link #hasNext()} == true once the first file is returned, then the
 * {@link #hasNewFile()} should be called to check if another file can be
 * created and re-allow {@link #hasNext()} to return true
 *
 * @author Antoine Willerval
 */
public class FileChunkIterator<E> implements Iterator<E> {
	private final ToLongFunction<E> estimationFunction;
	private final Iterator<E> it;
	private final long maxSize;
	private long totalSize = 0L;
	private long currentSize = 0L;
	private E next;
	private boolean stop = false;

	/**
	 * create a file iterator from a stream and a max size
	 *
	 * @param it                 the iterator
	 * @param maxSize            the maximum size of each file, this size is
	 *                           estimated, so files can be bigger.
	 * @param estimationFunction the element estimation function
	 */
	public FileChunkIterator(Iterator<E> it, long maxSize, ToLongFunction<E> estimationFunction) {
		this.it = it;
		this.maxSize = maxSize;
		this.estimationFunction = estimationFunction;
	}

	@Override
	public boolean hasNext() {
		if (stop)
			return false;

		if (next != null)
			return true;

		if (it.hasNext()) {
			next = it.next();
			long estimation = estimationFunction.applyAsLong(next);

			totalSize += estimation;

			if (currentSize + estimation >= maxSize) {
				stop = true;
				currentSize = estimation;
				return false;
			}

			currentSize += estimation;
			return true;
		}
		return false;
	}

	@Override
	public E next() {
		if (!hasNext()) {
			return null;
		}
		E t = next;
		next = null;
		return t;
	}

	@Override
	public void remove() {
		it.remove();
	}

	@Override
	public void forEachRemaining(Consumer<? super E> action) {
		it.forEachRemaining(action);
	}

	/**
	 * force the iterator to create a new file after the next hasNext()
	 */
	public void forceNewFile() {
		long estimation;
		if (next != null) {
			estimation = estimationFunction.applyAsLong(next);
		} else {
			estimation = 0;
		}
		currentSize = estimation;
		stop = true;
	}

	/**
	 * @return if we need to open a new file
	 */
	public boolean hasNewFile() {
		stop = false;
		return hasNext();
	}

	public long getTotalSize() {
		return totalSize;
	}
}

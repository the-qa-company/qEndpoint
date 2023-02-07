package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;

import java.util.Objects;

/**
 * ExceptionIterator Wrapper to notify a progress
 *
 * @param <T> iterator type
 * @param <E> iterator exception
 * @author Antoine WillervalF
 */
public class NotificationExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {
	private final ExceptionIterator<T, E> it;
	private final long size;
	private final long split;
	private final String message;
	private final ProgressListener listener;
	private long current = 0L;

	public NotificationExceptionIterator(ExceptionIterator<T, E> it, long size, long split, String message,
			ProgressListener listener) {
		this.it = Objects.requireNonNull(it, "it can't be null!");
		if (size < 0) {
			throw new IllegalArgumentException("size can't be negative!");
		}
		if (split < 0) {
			throw new IllegalArgumentException("split can't be negative! " + split);
		}
		// set size to be at least 1 to allow empty next() error
		this.size = Math.max(1, size);
		// minimize split by size to avoid dividing by 0, and avoid that it is
		// zero
		this.split = Math.max(1, Math.min(split, size));
		this.message = Objects.requireNonNull(message, "message can't be null!");
		this.listener = Objects.requireNonNullElseGet(listener, () -> (perc, msg) -> {});
	}

	@Override
	public boolean hasNext() throws E {
		return it.hasNext();
	}

	@Override
	public T next() throws E {
		current++;
		if (current % (size / split) == 0) {
			listener.notifyProgress(100f * current / size, message + " " + current + "/" + size);
		}
		return it.next();
	}

	@Override
	public void remove() throws E {
		it.remove();
	}

	@Override
	public long getSize() {
		return it.getSize();
	}
}

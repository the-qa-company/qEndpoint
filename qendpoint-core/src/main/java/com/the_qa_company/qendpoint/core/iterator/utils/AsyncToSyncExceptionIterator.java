package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;

public class AsyncToSyncExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {

	private final AsyncExceptionIterator<T, E> asyncIterator;
	private T nextValue;
	private boolean hasPrefetched = false;
	private boolean finished = false;

	public AsyncToSyncExceptionIterator(AsyncExceptionIterator<T, E> asyncIterator) {
		this.asyncIterator = asyncIterator;
	}

	@Override
	public boolean hasNext() throws E {
		if (finished) {
			return false;
		}
		// If we haven't prefetched yet, do so
		if (!hasPrefetched) {
			fetchNext();
			hasPrefetched = true;
		}
		return !finished;
	}

	@Override
	public T next() throws E {
		if (!hasNext()) {
			throw new NoSuchElementException("Iterator exhausted");
		}
		// Return the prefetched value
		T valueToReturn = nextValue;
		// Immediately fetch the next one
		fetchNext();
		return valueToReturn;
	}

	private void fetchNext() throws E {
		try {
			T result = asyncIterator.nextFuture().join();
			if (result == null) {
				finished = true;
				nextValue = null;
			} else {
				nextValue = result;
			}
		} catch (CompletionException ce) {
			Throwable cause = ce.getCause();
			if (cause instanceof Exception) {
				// noinspection unchecked
				throw (E) cause;
			}
			throw ce;
		}
	}
}

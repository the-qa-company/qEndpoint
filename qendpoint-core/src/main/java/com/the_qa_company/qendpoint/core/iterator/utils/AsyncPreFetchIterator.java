package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wrapper that pre-fetches from a synchronous ExceptionIterator
 * asynchronously. - Creates or uses a provided Executor (potentially a
 * VirtualThreadPerTaskExecutor). - Encourages calling cancel() or close() if
 * partial consumption is likely.
 */
class AsyncPreFetchIterator<T, E extends Exception> implements AsyncExceptionIterator<T, E>, AutoCloseable {

	private final ExceptionIterator<T, E> iterator;

	// Holds the current fetch (chained) so it happens strictly in sequence.
	private CompletableFuture<T> currentFetch;

	private final AtomicBoolean hasMore = new AtomicBoolean(true);
	private final AtomicBoolean cancelled = new AtomicBoolean(false);
	private final AtomicReference<Exception> exception = new AtomicReference<>(null);

	private final Executor executor;
	private final boolean ownExecutor;

	public AsyncPreFetchIterator(ExceptionIterator<T, E> iterator) {
		this(iterator, null);
	}

	public AsyncPreFetchIterator(ExceptionIterator<T, E> iterator, Executor executor) {
		this.iterator = iterator;
		if (executor == null) {
			this.executor = Executors.newVirtualThreadPerTaskExecutor();
			this.ownExecutor = true;
		} else {
			this.executor = executor;
			this.ownExecutor = false;
		}
		// Schedule the initial fetch once
		this.currentFetch = fetchNext(null);
	}

	/**
	 * nextFuture() returns the current future, then updates currentFetch so
	 * that the next fetch is chained after the current future completes.
	 */
	@Override
	public synchronized CompletableFuture<T> nextFuture() {
		CompletableFuture<T> result = currentFetch;

		// Chain the "next" fetch to happen strictly after the current result is
		// done:
		currentFetch = result.thenCompose(ignored -> fetchNext(null));
		return result;
	}

	/**
	 * A peek method if needed to see the current element without advancing.
	 * This is safe: we do not schedule an additional fetch unless nextFuture()
	 * is called.
	 */
	public synchronized CompletableFuture<T> peekFuture() {
		return currentFetch;
	}

	/**
	 * fetchNext() returns a future that (when run) checks hasNext() & next().
	 * It's always chained after the previous fetch, preventing parallel calls.
	 */
	private CompletableFuture<T> fetchNext(T ignored) {
		// If already cancelled or exception set, do not schedule more tasks.
		if (cancelled.getAcquire() || exception.getAcquire() != null) {
			return CompletableFuture.completedFuture(null);
		}
		return CompletableFuture.supplyAsync(() -> {
			try {
				if (iterator.hasNext()) {
					return iterator.next();
				} else {
					hasMore.setRelease(false);
					return null;
				}
			} catch (Exception ex) {
				exception.compareAndSet(null, ex);
				cancelled.setRelease(true);
				throw new CompletionException(ex);
			}
		}, executor);
	}

	/**
	 * Cancel and prevent further scheduling.
	 */
	public void cancel() {
		cancelled.setRelease(true);
		if (currentFetch != null) {
			currentFetch.cancel(true);
		}
	}

	/**
	 * Closes resources. If we own the executor, we shut it down.
	 */
	@Override
	public void close() {
		cancel();
		if (ownExecutor && executor instanceof ExecutorService) {
			((ExecutorService) executor).shutdownNow();
		}
	}
}

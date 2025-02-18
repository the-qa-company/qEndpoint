package com.the_qa_company.qendpoint.core.iterator.utils;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Abstraction for a concurrent stream of T. nextElement() => returns a Future
 * that completes with the next item, or null if done.
 */
interface MergeSource<T> {
	Future<T> nextElement() throws IOException;

	boolean hasMore() throws IOException;

	boolean exhausted();
}

/**
 * Leaf node that pulls from a single Iterator<T>. Each nextElement() spawns a
 * task that does iterator.next().
 */
class LeafSource<T> implements MergeSource<T> {
	private final ExceptionIterator<T, IOException> it;
	private final ForkJoinPool pool;
	private volatile boolean exhausted = false;

	LeafSource(ExceptionIterator<T, IOException> it, ForkJoinPool pool) {
		this.it = it;
		this.pool = pool;
	}

	@Override
	public synchronized boolean hasMore() {
		return !exhausted;
	}

	@Override
	public boolean exhausted() {
		if (exhausted)
			return true;
		try {
			if (!it.hasNext()) {
				return true;
			}
		} catch (Exception e) {

		}
		return exhausted;
	}

	@Override
	public Future<T> nextElement() {
		if (!hasMore()) {
			return CompletableFuture.completedFuture(null);
		}
		CompletableFuture<T> cf = new CompletableFuture<>();
		pool.submit(() -> {
			T val = null;
			synchronized (LeafSource.this) {
				try {
					if (!exhausted && it.hasNext()) {
						val = it.next();
					} else {
						exhausted = true;
					}
				} catch (Exception e) {
					exhausted = true;
					cf.completeExceptionally(e);
					return;
				}
			}
			cf.complete(val); // Will be null if exhausted
		});
		return cf;
	}
}

/**
 * A MergeNode that merges two children in parallel by prefetching into small
 * queues.
 */
class MergeNode<T> implements MergeSource<T> {
	private final MergeSource<T> left;
	private final MergeSource<T> right;
	private final Comparator<? super T> comp;
	private final ForkJoinPool pool;

	// Bounded queues to hold pre-fetched items from each child.
	// In practice you might pick a different capacity or structure.
	private final BlockingQueue<T> leftQueue;
	private final BlockingQueue<T> rightQueue;

	// Flags to indicate if we've exhausted each side.
	private volatile boolean leftExhausted = false;
	private volatile boolean rightExhausted = false;

	// Constant to define how many items we prefetch from each child at a time.
	private static final int PREFETCH_CAPACITY = 4;

	MergeNode(MergeSource<T> left, MergeSource<T> right, Comparator<? super T> comp, ForkJoinPool pool) {
		this.left = left;
		this.right = right;
		this.comp = comp;
		this.pool = pool;
		// A small queue for each side:
		this.leftQueue = new LinkedBlockingQueue<>(PREFETCH_CAPACITY);
		this.rightQueue = new LinkedBlockingQueue<>(PREFETCH_CAPACITY);

		// Kick off initial fill
		ensurePrefetch(left, leftQueue, () -> leftExhausted);
		ensurePrefetch(right, rightQueue, () -> rightExhausted);
	}

	/**
	 * We have more if either queue is non-empty or that side can still produce
	 * more.
	 */
	@Override
	public boolean hasMore() {
		if (!leftQueue.isEmpty() || !rightQueue.isEmpty()) {
			return true;
		}
		if ((leftExhausted || left.exhausted()) && (rightExhausted || right.exhausted())) {
			return false;
		}
		return true;
	}

	@Override
	public boolean exhausted() {
		return !hasMore();
	}

	@Override
	public Future<T> nextElement() throws IOException {
		if (!hasMore()) {
			return CompletableFuture.completedFuture(null);
		}

		CompletableFuture<T> cf = new CompletableFuture<>();

		// We'll pick from the heads of both queues (blocking if empty).
		// But to remain asynchronous, we do that in a pool thread:
		pool.submit(() -> {
			try {
				// Wait for an item from each queue if available, or null if
				// side is exhausted:
				left.exhausted();
				T leftVal = pollOrNull(leftQueue, () -> leftExhausted || left.exhausted());
				T rightVal = pollOrNull(rightQueue, () -> rightExhausted || right.exhausted());

				// If both sides are null => everything is exhausted
				if (leftVal == null && rightVal == null) {
					cf.complete(null);
					return;
				}
				if (leftVal != null && rightVal == null) {
					// only left side had an item
					cf.complete(leftVal);
					// Re‐prefetch next from left
					ensurePrefetch(left, leftQueue, () -> leftExhausted);
					return;
				}
				if (leftVal == null && rightVal != null) {
					// only right side had an item
					cf.complete(rightVal);
					ensurePrefetch(right, rightQueue, () -> rightExhausted);
					return;
				}
				// Both are non-null. Pick the smaller, put the other back in
				// its queue
				if (comp.compare(leftVal, rightVal) <= 0) {
					// leftVal is chosen
					cf.complete(leftVal);
					// Put the rightVal back into the rightQueue (front)
					rightQueue.put(rightVal);
					// Now we can refill left side again
					ensurePrefetch(left, leftQueue, () -> leftExhausted);
				} else {
					// rightVal is chosen
					cf.complete(rightVal);
					// Put the leftVal back
					leftQueue.put(leftVal);
					// Refill right side
					ensurePrefetch(right, rightQueue, () -> rightExhausted);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				cf.completeExceptionally(e);
			} catch (Exception e) {
				cf.completeExceptionally(e);
			}
		});

		return cf;
	}

	/**
	 * Poll one item from the queue. If the queue is empty but not exhausted, we
	 * block. If it's exhausted and empty, return null.
	 */
	private T pollOrNull(BlockingQueue<T> queue, Supplier<Boolean> isExhausted) throws InterruptedException {
		// If queue is non-empty, take() won't block long.
		// If it's empty but not exhausted, we might wait for the next item
		// (unless no more is coming).
		while (true) {
			if (!queue.isEmpty()) {
				return queue.take();
			}
			if (isExhausted.get()) {
				// The child can't produce more
				return null;
			}
			// If not exhausted and the queue is empty,
			// we wait a bit to see if new items arrive from prefetch
			// (Though typically ensurePrefetch will produce them soon.)
			Thread.sleep(1); // simplistic small sleep; or use e.g.
			// queue.poll(timeout)
		}
	}

	/**
	 * Ensures each child is prefetching new items up to the queue's capacity,
	 * asynchronously.
	 */
	private void ensurePrefetch(MergeSource<T> child, BlockingQueue<T> queue, Supplier<Boolean> exhaustedFlag) {
		if (exhaustedFlag.get()) {
			return; // already exhausted
		}

		// While the queue still has capacity, request the next item.
		// We'll do this in a loop (but asynchronously) so that we fill up to
		// capacity.
		pool.submit(() -> {
			try {
				while (!exhaustedFlag.get() && !exhausted() && queue.remainingCapacity() > 0) {

					// fetch next item
					Future<T> fut = child.nextElement();
					T val = fut.get(10, TimeUnit.SECONDS); // block in a pool
					// thread
					if (val == null) {
						// child exhausted
						setExhausted(exhaustedFlag);
						break;
					}
					queue.put(val);

				}
			} catch (Exception e) {
				// Mark exhausted or propagate error somehow
				setExhausted(exhaustedFlag);
			}
		});
	}

	private synchronized void setExhausted(Supplier<Boolean> exhaustedFlag) {
		// Ugly but easy: if exhaustedFlag points to leftExhausted, set it, else
		// set rightExhausted
		// A better design might store a boolean or do a callback.
		com.github.jsonldjava.shaded.com.google.common.base.Supplier<Boolean> isLeftExhausted = this::isLeftExhausted;
		if (exhaustedFlag == isLeftExhausted) {
			leftExhausted = true;
		} else {
			rightExhausted = true;
		}
	}

	private boolean isLeftExhausted() {
		return leftExhausted;
	}

	private boolean isRightExhausted() {
		return rightExhausted;
	}
}

/**
 * Build a balanced merge tree from a list of Iterators.
 */
class ParallelMergeBuilder {
	public static <T, E extends Exception> MergeSource<T> buildMergeTree(
			List<ExceptionIterator<T, IOException>> iterators, Comparator<? super T> comparator, ForkJoinPool pool) {

		int n = iterators.size();
		if (n == 0) {
			return new MergeSource<>() {
				@Override
				public Future<T> nextElement() {
					return CompletableFuture.completedFuture(null);
				}

				@Override
				public boolean hasMore() {
					return false;
				}

				@Override
				public boolean exhausted() {
					return true;
				}
			};
		}
		if (n == 1) {
			return new LeafSource<>(iterators.get(0), pool);
		}
		// Split in half
		int mid = n / 2;
		MergeSource<T> left = buildMergeTree(iterators.subList(0, mid), comparator, pool);
		MergeSource<T> right = buildMergeTree(iterators.subList(mid, n), comparator, pool);
		return new MergeNode<>(left, right, comparator, pool);
	}
}

/**
 * Convert a MergeSource<T> into a normal Iterator<T>.
 */
class ParallelMergeIterator<T> implements ExceptionIterator<T, IOException> {
	private final MergeSource<T> root;
	private T nextItem;

	ParallelMergeIterator(MergeSource<T> root) {
		this.root = root;
//		fetchNext();
	}

	private void fetchNext() throws IOException {
		if (!root.hasMore()) {
			nextItem = null;
			return;
		}
		try {
			nextItem = root.nextElement().get();
		} catch (InterruptedException | ExecutionException e) {
			if (e.getCause() instanceof IOException) {
				throw (IOException) e.getCause();
			}
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean hasNext() {
		return nextItem != null;
	}

	@Override
	public T next() throws IOException {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		T ret = nextItem;
		fetchNext();
		return ret;
	}
}

public class ParallelMerge {

	public static <T> ExceptionIterator<T, IOException> parallelMergeJoin(
			List<ExceptionIterator<T, IOException>> iterators, Comparator<T> comparator) {

		ForkJoinPool pool = ForkJoinPool.commonPool();
		MergeSource<T> root = ParallelMergeBuilder.buildMergeTree(iterators, comparator, pool);
		return new ParallelMergeIterator<>(root);
	}
}

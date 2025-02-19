package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * ParallelZipperAsyncIterator merges two sorted AsyncExceptionIterator sources
 * into a single sorted stream. Both sides always have exactly one fetch in
 * progress, which maximizes concurrency. The main steps in nextFuture(): 1.
 * Wait for leftCurrent and rightCurrent to complete, giving (leftVal,
 * rightVal). 2. Create a Result object indicating which value to return and
 * which side(s) to advance. 3. Then compose (flatten) that into a single future
 * that schedules new fetches for whichever side was consumed, and returns the
 * chosen value to the caller.
 */
public class ParallelZipperAsyncIterator<T, E extends Exception> implements AsyncExceptionIterator<T, E> {

	private final AsyncExceptionIterator<T, E> left; // The left input stream
	private final AsyncExceptionIterator<T, E> right; // The right input stream
	private final Comparator<T> comparator; // Comparator for sorting

	// Each side holds one "current" item in flight
	private CompletableFuture<T> leftCurrent;
	private CompletableFuture<T> rightCurrent;

	/**
	 * Constructs a ParallelZipperAsyncIterator from two AsyncExceptionIterator
	 * sources. Immediately fetches one element from each side.
	 */
	public ParallelZipperAsyncIterator(AsyncExceptionIterator<T, E> left, AsyncExceptionIterator<T, E> right,
			Comparator<T> comparator) {
		this.left = left;
		this.right = right;
		this.comparator = comparator;

		// Start fetching one item on each side
		this.leftCurrent = left.nextFuture();
		this.rightCurrent = right.nextFuture();
	}

	/**
	 * nextFuture(): 1. Waits for both sides' current futures to complete ->
	 * (leftVal, rightVal). 2. Decides which item to return and which side to
	 * advance, building a small Result object. 3. thenCompose on that Result to
	 * schedule side fetches if needed and return the chosen value.
	 */
	@Override
	public CompletableFuture<T> nextFuture() {
		// Combine the two futures to get leftVal and rightVal once both
		// complete
		CompletableFuture<Result> combined = leftCurrent.thenCombine(rightCurrent, (leftVal, rightVal) -> {
			if (leftVal == null && rightVal == null) {
				// Both sides exhausted
				return new Result(null, false, false);
			} else if (leftVal == null) {
				// Left exhausted, return rightVal, advance right
				return new Result(rightVal, false, true);
			} else if (rightVal == null) {
				// Right exhausted, return leftVal, advance left
				return new Result(leftVal, true, false);
			} else {
				// Both non-null, pick the smaller
				if (comparator.compare(leftVal, rightVal) <= 0) {
					// Use left
					return new Result(leftVal, true, false);
				} else {
					// Use right
					return new Result(rightVal, false, true);
				}
			}
		});

		// Now we flatten combined (a Future<Result>) into a Future<T>.
		// In the .thenCompose, we schedule new fetches for whichever side was
		// consumed
		// and return the chosen value (which might be null if both exhausted).
		return combined.thenCompose(res -> {
			// If res.value == null => both are exhausted
			if (res.value == null) {
				return CompletableFuture.completedFuture(null);
			} else {
				// If we used left side => schedule a new fetch for left
				if (res.advanceLeft) {
					leftCurrent = left.nextFuture();
				}
				// If we used right side => schedule a new fetch for right
				if (res.advanceRight) {
					rightCurrent = right.nextFuture();
				}
				return CompletableFuture.completedFuture(res.value);
			}
		}).exceptionally(ex -> {
			// If an exception occurs, rethrow it as a CompletionException
			throw new CompletionException(ex);
		});
	}

	/**
	 * A small helper class that indicates which item we decided to return, and
	 * whether we want to advance the left or right side.
	 */
	private class Result {
		final T value;
		final boolean advanceLeft;
		final boolean advanceRight;

		Result(T value, boolean advanceLeft, boolean advanceRight) {
			this.value = value;
			this.advanceLeft = advanceLeft;
			this.advanceRight = advanceRight;
		}
	}
}

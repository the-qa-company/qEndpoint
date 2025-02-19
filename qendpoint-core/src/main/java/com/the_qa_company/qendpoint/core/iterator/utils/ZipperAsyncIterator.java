package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Comparator;
import java.util.concurrent.CompletableFuture;

/**
 * ZipperAsyncIterator merges two sorted AsyncExceptionIterator streams into a
 * single sorted stream. It stores a "buffer" future for each side, retrieves
 * them in parallel, and compares their values.
 */
public class ZipperAsyncIterator<T, E extends Exception> implements AsyncExceptionIterator<T, E> {
	private final AsyncExceptionIterator<T, E> left;
	private final AsyncExceptionIterator<T, E> right;
	private final Comparator<T> comparator;

	// Buffers for each side. Each call to nextFuture() will compare these
	// values and advance the used side.
	private CompletableFuture<T> leftBuffer;
	private CompletableFuture<T> rightBuffer;

	/**
	 * Constructs a ZipperAsyncIterator from two AsyncExceptionIterators and a
	 * comparator. We initialize each side's buffer by calling nextFuture()
	 * once.
	 */
	public ZipperAsyncIterator(AsyncExceptionIterator<T, E> left, AsyncExceptionIterator<T, E> right,
			Comparator<T> comparator) {
		this.left = left;
		this.right = right;
		this.comparator = comparator;

		// Initialize each buffer with one fetched value.
		this.leftBuffer = left.nextFuture();
		this.rightBuffer = right.nextFuture();
	}

	/**
	 * nextFuture() returns a future that, when complete, yields the next merged
	 * element (or null if both sides are exhausted). We compare the two
	 * buffered values and advance only the side whose element is chosen.
	 */
	@Override
	public CompletableFuture<T> nextFuture() {
		// Combine the two buffer futures into a single future-of-a-future.
		// When both buffers resolve, compare them to see which side's value to
		// consume.
		CompletableFuture<CompletableFuture<T>> combined = leftBuffer.thenCombine(rightBuffer, (leftVal, rightVal) -> {
			if (leftVal == null && rightVal == null) {
				// Both sides are exhausted
				return CompletableFuture.completedFuture(null);
			} else if (leftVal == null) {
				// Left is exhausted, so return rightVal and advance right side
				CompletableFuture<T> toReturn = CompletableFuture.completedFuture(rightVal);
				rightBuffer = right.nextFuture(); // fetch the next from the
													// right
				return toReturn;
			} else if (rightVal == null) {
				// Right is exhausted, so return leftVal and advance left side
				CompletableFuture<T> toReturn = CompletableFuture.completedFuture(leftVal);
				leftBuffer = left.nextFuture(); // fetch the next from the left
				return toReturn;
			} else {
				// Both sides have a value; compare them
				if (comparator.compare(leftVal, rightVal) <= 0) {
					// left is smaller (or equal)
					CompletableFuture<T> toReturn = CompletableFuture.completedFuture(leftVal);
					leftBuffer = left.nextFuture(); // refill from left side
					return toReturn;
				} else {
					// right is smaller
					CompletableFuture<T> toReturn = CompletableFuture.completedFuture(rightVal);
					rightBuffer = right.nextFuture(); // refill from right side
					return toReturn;
				}
			}
		});

		// combined is a Future<Future<T>>. We flatten it to Future<T> with
		// thenCompose.
		return combined.thenCompose(f -> f);
	}
}

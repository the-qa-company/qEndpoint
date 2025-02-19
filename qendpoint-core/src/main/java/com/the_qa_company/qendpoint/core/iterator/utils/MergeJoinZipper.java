package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * MergeJoinZipper builds a balanced merge tree from a list of sorted
 * ExceptionIterator instances. The resulting ExceptionIterator is synchronous,
 * but internally uses asynchronous prefetching and merging.
 */
public class MergeJoinZipper {

	/**
	 * @param iterators  A list of sorted, synchronous ExceptionIterator
	 *                   objects.
	 * @param comparator The comparator used to merge them.
	 * @return A final ExceptionIterator that merges all input in sorted order.
	 */
	public static <T, E extends Exception> ExceptionIterator<T, E> buildMergeTree(
			List<ExceptionIterator<T, E>> iterators, Comparator<T> comparator) {
		if (iterators.isEmpty()) {
			return new EmptyExceptionIterator<>();
		}
		if (iterators.size() == 1) {
			return wrapAsync(iterators.get(0));
		}

		// 1) Wrap each synchronous iterator in an AsyncPreFetchIterator,
		// but store them in a list as AsyncExceptionIterator.
		List<AsyncExceptionIterator<T, E>> asyncIters = new ArrayList<>();
		for (ExceptionIterator<T, E> it : iterators) {
			asyncIters.add(new AsyncPreFetchIterator<>(it));
		}

		// 2) Pairwise merge them until only one remains
		while (asyncIters.size() > 1) {
			List<AsyncExceptionIterator<T, E>> merged = new ArrayList<>();
			for (int i = 0; i < asyncIters.size(); i += 2) {
				if (i + 1 < asyncIters.size()) {
					AsyncExceptionIterator<T, E> left = asyncIters.get(i);
					AsyncExceptionIterator<T, E> right = asyncIters.get(i + 1);
					// Now you can merge them in a ZipperAsyncIterator
					merged.add(new ParallelZipperAsyncIterator<>(left, right, comparator));
				} else {
					merged.add(asyncIters.get(i));
				}
			}
			asyncIters = merged;
		}

		// 3) Wrap the final AsyncExceptionIterator in a synchronous
		// AsyncToSyncExceptionIterator
		return new AsyncToSyncExceptionIterator<>(asyncIters.get(0));
	}

	/**
	 * Helper method for the single-iterator case.
	 */
	private static <T, E extends Exception> ExceptionIterator<T, E> wrapAsync(ExceptionIterator<T, E> iterator) {
		AsyncPreFetchIterator<T, E> async = new AsyncPreFetchIterator<>(iterator);
		return new AsyncToSyncExceptionIterator<>(async);
	}
}

/**
 * A simple empty iterator implementation (used if the list of iterators is
 * empty).
 */
class EmptyExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {
	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public T next() {
		throw new java.util.NoSuchElementException("Empty iterator");
	}
}

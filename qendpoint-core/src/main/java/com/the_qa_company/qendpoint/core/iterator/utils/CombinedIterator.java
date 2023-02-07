package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Combine multiple iterators, the hasNext method of the #i+1 won't be called
 * before the #i
 *
 * @param <T> iterator type
 * @author Antoine Willerval
 */
public class CombinedIterator<T> implements Iterator<T> {
	/**
	 * combine multiple iterators
	 *
	 * @param iterators iterators
	 * @param <T>       iterator type
	 * @return iterator
	 * @throws java.lang.NullPointerException if iterators is null
	 */
	public static <T> Iterator<T> combine(List<Iterator<T>> iterators) {
		Objects.requireNonNull(iterators, "iterators can't be null");
		return combine(iterators, 0, iterators.size());
	}

	private static <T> Iterator<T> combine(List<Iterator<T>> iterators, int start, int end) {
		int len = end - start;

		if (len <= 0) {
			return new Iterator<>() {
				@Override
				public boolean hasNext() {
					return false;
				}

				@Override
				public T next() {
					return null;
				}
			};
		}
		if (len == 1) {
			return iterators.get(start);
		}

		// use a tree to reduce the effect of big combine
		int mid = (end + start) / 2;
		return new CombinedIterator<>(combine(iterators, start, mid), combine(iterators, mid, end));
	}

	private final Iterator<T> left;
	private final Iterator<T> right;
	private T next;

	private CombinedIterator(Iterator<T> left, Iterator<T> right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
		}

		if (left.hasNext()) {
			next = left.next();
		} else if (right.hasNext()) {
			next = right.next();
		} else {
			return false;
		}
		return true;
	}

	@Override
	public T next() {
		if (!hasNext()) {
			return null;
		}
		try {
			return next;
		} finally {
			next = null;
		}
	}
}

package com.the_qa_company.qendpoint.core.iterator.utils;

public interface Reducer<T> {
	/**
	 * Reduce a and b into a (new) objects. Returns null if cannot reduce
	 * (different)
	 *
	 * @param a a
	 * @param b b
	 */
	T reduce(T a, T b);
}

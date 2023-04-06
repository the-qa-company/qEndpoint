package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.List;

/**
 * Combine multiple iterator
 *
 * @param <E> iterator type
 * @author Antoine Willerval
 */
public class CatIterator<E> extends FetcherIterator<E> {
	/**
	 * create iterator
	 *
	 * @param its iterators
	 * @param <E> iterator type
	 * @return iterator
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <E> Iterator<? extends E> of(Iterator<? extends E>... its) {
		return of(List.of(its));
	}
	/**
	 * create iterator
	 *
	 * @param its iterators
	 * @param <E> iterator type
	 * @return iterator
	 */
	public static <E> Iterator<? extends E> of(List<? extends Iterator<? extends E>> its) {
		// handle easy cases
		if (its.isEmpty()) {
			return EmptyIterator.of();
		}
		if (its.size() == 1) {
			return its.get(0);
		}
		return new CatIterator<>(its);
	}

	private final List<? extends Iterator<? extends E>> iterators;
	private int index;

	private CatIterator(List<? extends Iterator<? extends E>> iterators) {
		this.iterators = iterators;
	}

	@Override
	protected E getNext() {
		while (index < iterators.size()) {
			if (iterators.get(index).hasNext()) {
				return iterators.get(index).next();
			}
			index++;
		}
		return null;
	}

}

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
	 * @param <E> iterator type
	 * @return iterator
	 */
	public static <E> Iterator<? extends E> of() {
		return EmptyIterator.of();
	}

	/**
	 * create iterator
	 *
	 * @param it1 Iterator 1
	 * @param <E> iterator type
	 * @return iterator
	 */
	public static <E> Iterator<? extends E> of(Iterator<? extends E> it1) {
		return it1;
	}

	/**
	 * create iterator
	 *
	 * @param it1 Iterator 1
	 * @param it2 Iterator 2
	 * @param <E> iterator type
	 * @return iterator
	 */
	public static <E> Iterator<? extends E> of(Iterator<? extends E> it1, Iterator<? extends E> it2) {
		return of(List.of(it1, it2));
	}

	/**
	 * create iterator
	 *
	 * @param it1 Iterator 1
	 * @param it2 Iterator 2
	 * @param it3 Iterator 3
	 * @param <E> iterator type
	 * @return iterator
	 */
	public static <E> Iterator<? extends E> of(Iterator<? extends E> it1, Iterator<? extends E> it2, Iterator<? extends E> it3) {
		return of(List.of(it1, it2, it3));
	}

	/**
	 * create iterator
	 *
	 * @param it1 Iterator 1
	 * @param it2 Iterator 2
	 * @param it3 Iterator 3
	 * @param it4 Iterator 4
	 * @param <E> iterator type
	 * @return iterator
	 */
	public static <E> Iterator<? extends E> of(Iterator<? extends E> it1, Iterator<? extends E> it2, Iterator<? extends E> it3, Iterator<? extends E> it4) {
		return of(List.of(it1, it2, it3, it4));
	}

	/**
	 * create iterator
	 *
	 * @param it1 Iterator 1
	 * @param it2 Iterator 2
	 * @param it3 Iterator 3
	 * @param it4 Iterator 4
	 * @param it5 Iterator 5
	 * @param <E> iterator type
	 * @return iterator
	 */
	public static <E> Iterator<? extends E> of(Iterator<? extends E> it1, Iterator<? extends E> it2, Iterator<? extends E> it3, Iterator<? extends E> it4, Iterator<? extends E> it5) {
		return of(List.of(it1, it2, it3, it4, it5));
	}

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

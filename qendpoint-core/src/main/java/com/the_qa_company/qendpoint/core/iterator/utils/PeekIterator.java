package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.function.Function;

public interface PeekIterator<T> extends Iterator<T> {

	/**
	 * create if required a peek iterator from this element
	 *
	 * @param it iterator
	 * @return element
	 * @param <T> iterator type
	 */
	static <T extends CharSequence> PeekIterator<T> of(Iterator<T> it) {
		if (it instanceof PeekIterator<T> it2) {
			return it2;
		}
		return new PeekIteratorImpl<>(it);
	}

	/**
	 * @return peek the element without passing to the next element
	 */
	T peek();

	/**
	 * map this iterator
	 *
	 * @param mappingFunction func
	 * @param <M>             new type
	 * @return iterator
	 */
	default <M> Iterator<M> map(Function<T, M> mappingFunction) {
		return new MapIterator<>(this, mappingFunction);
	}
}

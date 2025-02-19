package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.function.Function;

public interface PeekExceptionIterator<T, E extends Exception> extends ExceptionIterator<T, E> {
	/**
	 * create if required a peek iterator from this element
	 *
	 * @param it iterator
	 * @return element
	 * @param <T> iterator type
	 */
	static <T, E extends Exception> PeekExceptionIterator<T, E> of(ExceptionIterator<T, E> it) {
		if (it instanceof PeekExceptionIterator<T, E> it2) {
			return it2;
		}
		return new PeekExceptionIteratorImpl<>(it);
	}

	/**
	 * @return peek the element without passing to the next element
	 */
	T peek() throws E;
}

package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.function.Function;

public class FilterMapExceptionIterator<M, N, E extends Exception> implements ExceptionIterator<N, E> {
	public static <M, N> MapIterator<M, N> of(Iterator<M> base, Function<M, N> mappingFunction) {
		return new MapIterator<>(base, mappingFunction);
	}

	public static <M, N> MapIterator<M, N> of(Iterator<M> base, MapIterator.MapWithIdFunction<M, N> mappingFunction) {
		return new MapIterator<>(base, mappingFunction);
	}

	private final MapWithIdFunction<M, N, E> mappingFunction;
	private final ExceptionIterator<M, E> base;
	private long index;
	private N next;

	public FilterMapExceptionIterator(ExceptionIterator<M, E> base, ExceptionFunction<M, N, E> mappingFunction) {
		this(base, (m, i) -> mappingFunction.apply(m));
	}

	public FilterMapExceptionIterator(ExceptionIterator<M, E> base, MapWithIdFunction<M, N, E> mappingFunction) {
		this.base = base;
		this.mappingFunction = mappingFunction;
	}

	@Override
	public boolean hasNext() throws E {
		if (next != null) {
			return true;
		}
		while (base.hasNext()) {
			N next = mappingFunction.apply(base.next(), index++);
			if (next != null) {
				this.next = next;
				return true;
			}
		}
		return false;
	}

	@Override
	public N next() throws E {
		if (!hasNext()) {
			return null;
		}
		try {
			return next;
		} finally {
			next = null;
		}
	}

	@Override
	public void remove() throws E {
		base.remove();
	}

	@FunctionalInterface
	public interface MapWithIdFunction<M, N, E extends Exception> {
		N apply(M element, long index) throws E;
	}

	@FunctionalInterface
	public interface ExceptionFunction<M, N, E extends Exception> {
		N apply(M element) throws E;
	}
}

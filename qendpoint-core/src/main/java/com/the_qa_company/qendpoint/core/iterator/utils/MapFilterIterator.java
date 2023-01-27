package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Iterator to map and filter values, null values are ignored by the iterator
 *
 * @param <N> iterator type
 * @author Antoine Willerval
 */
public class MapFilterIterator<M, N> implements Iterator<N> {
	public static <M, N> MapFilterIterator<M, N> of(Iterator<M> base, Function<M, N> mappingFunction) {
		return new MapFilterIterator<>(base, mappingFunction);
	}

	public static <M, N> MapFilterIterator<M, N> of(Iterator<M> base,
			MapIterator.MapWithIdFunction<M, N> mappingFunction) {
		return new MapFilterIterator<>(base, mappingFunction);
	}

	private final MapIterator.MapWithIdFunction<M, N> mappingFunction;
	private final Iterator<M> base;
	private long index;
	private N next;

	private MapFilterIterator(Iterator<M> base, Function<M, N> mappingFunction) {
		this(base, (m, i) -> mappingFunction.apply(m));
	}

	private MapFilterIterator(Iterator<M> base, MapIterator.MapWithIdFunction<M, N> mappingFunction) {
		this.base = base;
		this.mappingFunction = mappingFunction;
	}

	@Override
	public boolean hasNext() {
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
	public N next() {
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
	public void remove() {
		base.remove();
	}

	/**
	 * @return this iterator, but as an exception iterator
	 */
	public ExceptionIterator<N, RuntimeException> asExceptionIterator() {
		return ExceptionIterator.of(this);
	}
}

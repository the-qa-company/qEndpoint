package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Iterator to map a value to another
 *
 * @param <M> origin type
 * @param <N> return type
 * @author Antoine Willerval
 */
public class MapIterator<M, N> implements Iterator<N> {
	public static <M, N> MapIterator<M, N> of(Iterator<M> base, Function<M, N> mappingFunction) {
		return new MapIterator<>(base, mappingFunction);
	}

	public static <M, N> MapIterator<M, N> of(Iterator<M> base, MapWithIdFunction<M, N> mappingFunction) {
		return new MapIterator<>(base, mappingFunction);
	}

	private final MapWithIdFunction<M, N> mappingFunction;
	private final Iterator<M> base;
	private long index;

	public MapIterator(Iterator<M> base, Function<M, N> mappingFunction) {
		this(base, (m, i) -> mappingFunction.apply(m));
	}

	public MapIterator(Iterator<M> base, MapWithIdFunction<M, N> mappingFunction) {
		this.base = base;
		this.mappingFunction = mappingFunction;
	}

	@Override
	public boolean hasNext() {
		return base.hasNext();
	}

	@Override
	public N next() {
		return mappingFunction.apply(base.next(), index++);
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

	@FunctionalInterface
	public interface MapWithIdFunction<T, E> {
		E apply(T element, long index);
	}
}

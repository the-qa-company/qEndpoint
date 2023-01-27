package com.the_qa_company.qendpoint.core.iterator.utils;

/**
 * Exception Iterator to map a value to another
 *
 * @param <M> origin type
 * @param <N> return type
 * @param <E> the allowed exception
 * @author Antoine Willerval
 */
public class MapExceptionIterator<M, N, E extends Exception> implements ExceptionIterator<N, E> {
	private final MapWithIdFunction<M, N, E> mappingFunction;
	private final ExceptionIterator<M, E> base;
	private long index;

	public MapExceptionIterator(ExceptionIterator<M, E> base, ExceptionFunction<M, N, E> mappingFunction) {
		this(base, (m, i) -> mappingFunction.apply(m));
	}

	public MapExceptionIterator(ExceptionIterator<M, E> base, MapWithIdFunction<M, N, E> mappingFunction) {
		this.base = base;
		this.mappingFunction = mappingFunction;
	}

	@Override
	public boolean hasNext() throws E {
		return base.hasNext();
	}

	@Override
	public N next() throws E {
		return mappingFunction.apply(base.next(), index++);
	}

	@Override
	public void remove() throws E {
		base.remove();
	}

	@Override
	public long getSize() {
		return base.getSize();
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

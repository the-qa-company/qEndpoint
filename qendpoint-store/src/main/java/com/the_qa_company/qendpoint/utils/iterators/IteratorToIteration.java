package com.the_qa_company.qendpoint.utils.iterators;

import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;

public class IteratorToIteration<T, E extends Exception> implements ExceptionIterator<T, E>, AutoCloseable {
	private final CloseableIteration<T> delegate;

	public IteratorToIteration(CloseableIteration<T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public boolean hasNext() throws E {
		return delegate.hasNext();
	}

	@Override
	public T next() throws E {
		return delegate.next();
	}

	@Override
	public void close() throws E {
		delegate.close();
	}
}

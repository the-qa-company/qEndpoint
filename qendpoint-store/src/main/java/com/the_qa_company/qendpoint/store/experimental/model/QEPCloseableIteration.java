package com.the_qa_company.qendpoint.store.experimental.model;

import com.the_qa_company.qendpoint.core.storage.iterator.AutoCloseableGeneric;
import com.the_qa_company.qendpoint.core.storage.iterator.CloseableIterator;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;

@SuppressWarnings("deprecation")
public class QEPCloseableIteration<T, E extends Exception> implements CloseableIteration<T, E> {


	public static <T, E extends Exception> QEPCloseableIteration<T, E> of(CloseableIterator<T, E> it) {
		return new QEPCloseableIteration<>(it);
	}
	public static <T, E extends Exception> QEPCloseableIteration<T, E> of() {
		return of(CloseableIterator.empty());
	}
	private final CloseableIterator<T, E> it;

	private QEPCloseableIteration(CloseableIterator<T, E> it) {
		this.it = it;
	}

	@Override
	public void close() throws E {
		it.close();
	}

	@Override
	public boolean hasNext() throws E {
		return it.hasNext();
	}

	@Override
	public T next() throws E {
		return it.next();
	}

	@Override
	public void remove() throws E {
		it.remove();
	}
}

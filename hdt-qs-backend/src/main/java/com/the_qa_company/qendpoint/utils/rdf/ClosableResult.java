package com.the_qa_company.qendpoint.utils.rdf;

import org.eclipse.rdf4j.query.QueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;

public class ClosableResult<T extends QueryResult<E>, E> implements AutoCloseable {
	private final T query;
	private final RepositoryConnection closeable;

	public ClosableResult(T query, RepositoryConnection closeable) {
		this.query = query;
		this.closeable = closeable;
	}

	public T getQuery() {
		return query;
	}

	public RepositoryConnection getCloseable() {
		return closeable;
	}

	@Override
	public void close() throws RepositoryException {
		closeable.close();
	}
}

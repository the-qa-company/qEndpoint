package com.the_qa_company.qendpoint.utils.rdf;

import org.eclipse.rdf4j.query.QueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;

/**
 * Closable query result
 *
 * @param <T> the query result type
 * @param <E> the query result line type
 */
public class ClosableResult<T extends QueryResult<E>, E> implements AutoCloseable {
	private final T query;
	private final RepositoryConnection closeable;

	public ClosableResult(T query, RepositoryConnection closeable) {
		this.query = query;
		this.closeable = closeable;
	}

	/**
	 * @return the result
	 */
	public T getQuery() {
		return query;
	}

	/**
	 * @return the connection of this query
	 */
	public RepositoryConnection getCloseable() {
		return closeable;
	}

	@Override
	public void close() throws RepositoryException {
		closeable.close();
	}
}

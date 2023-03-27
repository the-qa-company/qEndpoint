package com.the_qa_company.qendpoint.utils.rdf;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResult;

/**
 * simple query result for boolean values
 *
 * @author Antoine Willerval
 */
public class BooleanQueryResult implements QueryResult<BooleanQueryResult> {
	private final boolean value;
	private boolean read;

	public BooleanQueryResult(boolean value) {
		this.value = value;
	}

	@Override
	public void close() throws QueryEvaluationException {
	}

	/**
	 * @return the returned value
	 */
	public boolean getValue() {
		return value;
	}

	@Override
	public boolean hasNext() throws QueryEvaluationException {
		return !read;
	}

	@Override
	public BooleanQueryResult next() throws QueryEvaluationException {
		read = true;
		return this;
	}

	@Override
	public void remove() throws QueryEvaluationException {
		throw new IllegalArgumentException("not implemented");
	}
}

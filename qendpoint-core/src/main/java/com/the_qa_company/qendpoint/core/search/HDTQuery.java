package com.the_qa_company.qendpoint.core.search;

import com.the_qa_company.qendpoint.core.search.component.HDTComponentTriple;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Query over the HDT
 *
 * @author Antoine Willerval
 */
public interface HDTQuery {
	/**
	 * @return run the query over the HDT
	 */
	Iterator<HDTQueryResult> query();

	/**
	 * @return the names of the named variable of this query
	 */
	Set<String> getVariableNames();

	List<HDTComponentTriple> getPatterns();

	/**
	 * set the timeout for this query
	 *
	 * @param millis milliseconds between the start and the end of the timeout,
	 *               0 for infinite
	 */
	void setTimeout(long millis);

	/**
	 * @return millis count of the timeout, 0 for infinite
	 */
	long getTimeout();
}

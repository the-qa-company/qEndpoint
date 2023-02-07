package com.the_qa_company.qendpoint.core.search;

import com.the_qa_company.qendpoint.core.search.component.HDTConstant;
import com.the_qa_company.qendpoint.core.search.component.HDTVariable;

import java.util.Set;

/**
 * Query result
 *
 * @author Antoine Willerval
 */
public interface HDTQueryResult {
	/**
	 * get a component of the query
	 *
	 * @param variableName name of the variable
	 * @return value
	 */
	HDTConstant getComponent(String variableName);

	/**
	 * get a component of the query
	 *
	 * @param variable variable
	 * @return value
	 */
	default HDTConstant getComponent(HDTVariable variable) {
		return getComponent(variable.getName());
	}

	/**
	 * @return the names of the variables in this query
	 */
	Set<String> getVariableNames();

	/**
	 * @return a copy of the query result
	 */
	HDTQueryResult copy();
}

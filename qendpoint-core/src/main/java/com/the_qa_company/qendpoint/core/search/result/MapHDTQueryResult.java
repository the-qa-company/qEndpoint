package com.the_qa_company.qendpoint.core.search.result;

import com.the_qa_company.qendpoint.core.search.HDTQueryResult;
import com.the_qa_company.qendpoint.core.search.component.HDTConstant;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link HDTQueryResult} using a hash map
 *
 * @author Antoine Willerval
 */
public class MapHDTQueryResult implements HDTQueryResult {
	private final Map<String, HDTConstant> results = new HashMap<>();

	public void set(String variableName, HDTConstant value) {
		results.put(variableName, value);
	}

	@Override
	public HDTConstant getComponent(String variableName) {
		return results.get(variableName);
	}

	@Override
	public Set<String> getVariableNames() {
		return results.keySet();
	}

	/**
	 * add all the other variables from another result without doing any copy
	 *
	 * @param other other result
	 * @see #addDeep(MapHDTQueryResult)
	 */
	public void add(MapHDTQueryResult other) {
		this.results.putAll(other.results);
	}

	/**
	 * add all the other variables from another result with a deep copy
	 *
	 * @param other other result
	 * @see #add(MapHDTQueryResult)
	 */
	public void addDeep(MapHDTQueryResult other) {
		// faster than map and put
		results.putAll(other.results);
		results.replaceAll((str, cst) -> cst.copy());
	}

	/**
	 * @return a deep copy of the results
	 */
	@Override
	public MapHDTQueryResult copy() {
		MapHDTQueryResult copy = new MapHDTQueryResult();
		copy.addDeep(copy);
		return copy;
	}

	@Override
	public String toString() {
		return results.toString();
	}
}

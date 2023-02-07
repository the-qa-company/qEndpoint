package com.the_qa_company.qendpoint.core.search.result;

import com.the_qa_company.qendpoint.core.search.HDTQueryResult;
import com.the_qa_company.qendpoint.core.search.component.HDTConstant;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
	 * add all the other variables from another result
	 *
	 * @param other other result
	 */
	public void add(MapHDTQueryResult other) {
		this.results.putAll(other.results);
	}

	/**
	 * @return a copy of the results
	 */
	@Override
	public MapHDTQueryResult copy() {
		MapHDTQueryResult copy = new MapHDTQueryResult();
		copy.add(this);
		return copy;
	}

	@Override
	public String toString() {
		return results.toString();
	}
}

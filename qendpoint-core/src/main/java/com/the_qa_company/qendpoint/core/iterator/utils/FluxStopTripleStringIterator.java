package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.util.Iterator;

public interface FluxStopTripleStringIterator extends Iterator<TripleString> {

	/**
	 * @return if a new flux can be extracted, will restart the flux
	 */
	boolean hasNextFlux();

	default boolean supportCount() {
		return false;
	}

	default long getTotalCount() {
		return 0;
	}
}

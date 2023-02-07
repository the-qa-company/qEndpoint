package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.Iterator;

/**
 * Utility class to generate triples
 */
public class TripleGenerator implements Iterator<TripleID> {
	private final long triples;
	private long current = 1;

	public TripleGenerator(long triples) {
		this.triples = triples;
	}

	@Override
	public boolean hasNext() {
		return current <= triples;
	}

	@Override
	public TripleID next() {
		long c = current++;
		return new TripleID(c, c, c);
	}
}

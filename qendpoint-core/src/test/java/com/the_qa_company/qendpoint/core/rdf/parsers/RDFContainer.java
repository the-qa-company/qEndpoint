package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RDFContainer implements RDFParserCallback.RDFCallback {
	private final Set<TripleString> triples = ConcurrentHashMap.newKeySet();

	@Override
	public void processTriple(TripleString triple, long pos) {
		// clone the triple
		triples.add(triple.tripleToString());
	}

	public Set<TripleString> getTriples() {
		return triples;
	}
}

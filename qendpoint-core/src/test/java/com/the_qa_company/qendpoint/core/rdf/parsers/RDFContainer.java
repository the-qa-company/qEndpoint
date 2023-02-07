package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.util.HashSet;
import java.util.Set;

public class RDFContainer implements RDFParserCallback.RDFCallback {
	private final Set<TripleString> triples = new HashSet<>();

	@Override
	public void processTriple(TripleString triple, long pos) {
		// clone the triple
		triples.add(triple.tripleToString());
	}

	public Set<TripleString> getTriples() {
		return triples;
	}
}

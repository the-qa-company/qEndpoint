package com.the_qa_company.qendpoint.core.rdf;

import java.io.IOException;

import com.the_qa_company.qendpoint.core.triples.TripleString;

public interface TripleWriter extends AutoCloseable {
	void addTriple(TripleString str) throws IOException;
}

package com.the_qa_company.qendpoint.core.rdf;

import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.io.IOException;

public interface TripleWriter extends AutoCloseable {
	void addTriple(TripleString str) throws IOException;
}

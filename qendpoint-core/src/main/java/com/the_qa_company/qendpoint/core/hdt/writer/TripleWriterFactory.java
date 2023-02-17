package com.the_qa_company.qendpoint.core.hdt.writer;

import com.the_qa_company.qendpoint.core.rdf.TripleWriter;

import java.io.OutputStream;

public class TripleWriterFactory {

	public static TripleWriter getWriter(OutputStream out) {
		return new TripleWriterNtriples(out);
	}
}

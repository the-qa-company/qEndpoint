package com.the_qa_company.qendpoint.core.hdt.writer;

import java.io.IOException;
import java.io.OutputStream;

import com.the_qa_company.qendpoint.core.rdf.TripleWriter;

public class TripleWriterFactory {

	public static TripleWriter getWriter(OutputStream out) {
		return new TripleWriterNtriples(out);
	}
}

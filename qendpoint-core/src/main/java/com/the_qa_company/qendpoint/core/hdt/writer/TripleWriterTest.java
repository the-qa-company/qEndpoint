package com.the_qa_company.qendpoint.core.hdt.writer;

import com.the_qa_company.qendpoint.core.rdf.TripleWriter;
import com.the_qa_company.qendpoint.core.triples.TripleString;

public class TripleWriterTest {

	public static void main(String[] args) throws Exception {
//		TripleWriter writer = TripleWriterFactory.getWriter("out.nt.gz", true);
		try (TripleWriter writer = TripleWriterFactory.getWriter(System.out)) {
			TripleString ts = new TripleString("http://example.org/hello", "http://example.org/mypred", "\"Myvalue\"");

			writer.addTriple(ts);
		}
	}

}

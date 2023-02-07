package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;

public class RDFParserRIOTTest extends AbstractNTriplesParserTest {

	@Override
	protected RDFParserCallback createParser() {
		return new RDFParserRIOT();
	}
}

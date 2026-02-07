package com.the_qa_company.qendpoint.core.rdf;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserSimple;
import org.junit.Assert;
import org.junit.Test;

public class RDFParserFactoryDefaultTest {
	@Test
	public void nquadsUsesSimpleParserByDefault() {
		RDFParserCallback parser = RDFParserFactory.getParserCallback(RDFNotation.NQUAD);
		Assert.assertTrue("Expected RDFParserSimple for NQUAD by default", parser instanceof RDFParserSimple);
	}
}

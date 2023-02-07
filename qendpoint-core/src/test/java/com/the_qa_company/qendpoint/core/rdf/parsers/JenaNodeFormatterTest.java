package com.the_qa_company.qendpoint.core.rdf.parsers;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.NodeFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JenaNodeFormatterTest {

	@Test
	public void testBlankNode() {
		assertEquals("_:x", JenaNodeFormatter.format(NodeFactory.createBlankNode("x")));
	}

	@Test
	public void testUri() {
		assertEquals("x", JenaNodeFormatter.format(NodeFactory.createURI("x")));
	}

	@Test
	public void testPlainLiteral() {
		assertEquals("\"x\"", JenaNodeFormatter.format(NodeFactory.createLiteral("x")));
	}

	@Test
	public void testLangLiteral() {
		assertEquals("\"x\"@en", JenaNodeFormatter.format(NodeFactory.createLiteral("x", "en")));
	}

	@Test
	public void testLangLiteralCaseInsensitive() {
		assertEquals("\"x\"@en", JenaNodeFormatter.format(NodeFactory.createLiteral("x", "EN")));
	}

	@Test
	public void testTypedLiteral() {
		assertEquals("\"1\"^^<http://www.w3.org/2001/XMLSchema#int>",
				JenaNodeFormatter.format(NodeFactory.createLiteralByValue(1, XSDDatatype.XSDint)));
	}
}

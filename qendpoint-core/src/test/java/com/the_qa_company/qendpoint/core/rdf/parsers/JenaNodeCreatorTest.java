package com.the_qa_company.qendpoint.core.rdf.parsers;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.jena.graph.NodeFactory.createBlankNode;
import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createLiteralByValue;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class JenaNodeCreatorTest {

	public Node[] blankNodeParams() {
		return new Node[] { createBlankNode(), createBlankNode("1-2-Z"), createBlankNode("123"),
				createBlankNode("abc"), };
	}

	public Node[] uriParams() {
		return new Node[] { createURI(""), createURI("abc"), createURI("http://example.com?query#anchor"),
				createURI("http://example.com?query#ånchor"), };
	}

	public Node[] literalParams() {
		return new Node[] { createLiteral(""), createLiteral("\"\""), createLiteral("\"\"^^"), createLiteral("\"\"@"),
				createLiteral("^^"), createLiteral("@"), createLiteral("''"), createLiteral("abc"),
				createLiteral("'abc'"), createLiteral("\"abc\""), createLiteral("\"\"\"abc\"\"\""),
				createLiteral("'''abc'''"), createLiteral("\\\"abc\\'"), createLiteral("'abc\t\ndef'"),
				createLiteral("'abc\u00c3\u00b3def'"), createLiteral("\\u0020"), createLiteral("abc\"@\"en"),
				createLiteral("\"abc\"@\"en\""), createLiteral("\"abc\"^^<xyz>"), createLiteral("abc", "en"),
				createLiteral("\"123\"", "en-uk"),
				createLiteral("abc", TypeMapper.getInstance().getSafeTypeByName("en")),
				createLiteral("\"123\"", TypeMapper.getInstance().getSafeTypeByName("xyz")),
				createLiteral("abc", TypeMapper.getInstance().getSafeTypeByName("http://example.com#z\u00c3\u00b3")),
				createLiteralByValue(1.3e67, XSDDatatype.XSDdouble), createLiteralByValue(123, XSDDatatype.XSDint),
				createLiteralByValue(-456, XSDDatatype.XSDint), };
	}

	@Test
	@Parameters(method = "blankNodeParams")
	public void roundTripBlankNode(Node node) {
		doRoundTrip(node);
	}

	@Test
	@Parameters(method = "uriParams")
	public void roundTripUri(Node node) {
		doRoundTrip(node);
	}

	@Test
	@Parameters(method = "literalParams")
	public void roundTripLiteral(Node node) {
		doRoundTrip(node);
	}

	private void doRoundTrip(Node node) {
		String string = JenaNodeFormatter.format(node);
		Node result = JenaNodeCreator.create(string);
		assertEquals(node, result);
	}
}

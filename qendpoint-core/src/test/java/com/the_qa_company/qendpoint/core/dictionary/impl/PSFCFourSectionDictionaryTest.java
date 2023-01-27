package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.rdf.parsers.JenaNodeFormatter;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.NodeFactory;
import org.junit.Test;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.util.string.DelayedString;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class PSFCFourSectionDictionaryTest {

	@Test
	public void testUri() {
		doRoundTripNoop("http://example.com#s");
	}

	@Test
	public void testBracketedUri() {
		doRoundTripNoop("<http://example.com#s>");
	}

	@Test
	public void testBlankNode() {
		doRoundTripNoop("_:abc");
	}

	@Test
	public void testPlainLiteral() {
		doRoundTripNoop("\"abc\"");
	}

	@Test
	public void testLocalizedLiteral() {
		String literal = JenaNodeFormatter.format(NodeFactory.createLiteral("abc", "en"));
		doRoundTrip(literal, "@en\"abc\"");
	}

	@Test
	public void testTypedLiteral() {
		String literal = JenaNodeFormatter.format(NodeFactory.createLiteral("123", XSDDatatype.XSDinteger));
		doRoundTrip(literal, "^^<http://www.w3.org/2001/XMLSchema#integer>\"123\"");
	}

	private void doRoundTripNoop(String original) {
		doRoundTrip(original, original);
	}

	private void doRoundTrip(String original, String expectedEncoded) {
		doRoundTripString(original, expectedEncoded);
		doRoundTripCompact(new CompactString(original), new CompactString(expectedEncoded));
	}

	private void doRoundTripString(String original, String expectedEncoded) {
		String encoded = (String) PSFCFourSectionDictionary.encode(original);
		assertEquals(expectedEncoded, encoded);

		String decoded = (String) PSFCFourSectionDictionary.decode(encoded);
		assertEquals(original, decoded);

		// If encoding had no effect, we should have returned the original
		// CharSequence object
		if (original.equals(encoded)) {
			assertSame(original, encoded);
		}
	}

	private void doRoundTripCompact(CompactString original, CompactString expectedEncoded) {
		CompactString encoded = (CompactString) DelayedString.unwrap(PSFCFourSectionDictionary.encode(original));
		assertEquals(expectedEncoded, encoded);

		CompactString decoded = (CompactString) DelayedString.unwrap(PSFCFourSectionDictionary.decode(encoded));
		assertEquals(original, decoded);

		// If encoding had no effect, we should have returned the original
		// CharSequence object
		if (original.equals(encoded)) {
			assertSame(original, encoded);
		}
	}
}

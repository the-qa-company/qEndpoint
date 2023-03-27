package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RDFStreamUtilsTest {
	@Test
	public void convertTest() {
		SimpleValueFactory vf = SimpleValueFactory.getInstance();

		assertEquals(Values.iri("http://example.org/#me"),
				RDFStreamUtils.convertCharSequence(vf, ByteString.of("http://example.org/#me")));

		assertEquals(Values.iri("http://example.org/#me"),
				RDFStreamUtils.convertCharSequence(vf, ByteString.of("<http://example.org/#me>")));

		assertEquals(Values.bnode("coolnode"), RDFStreamUtils.convertCharSequence(vf, ByteString.of("_:coolnode")));

		assertEquals(Values.literal("aaa"), RDFStreamUtils.convertCharSequence(vf, ByteString.of("\"aaa\"")));

		assertEquals(Values.literal("aaa", "en-us"),
				RDFStreamUtils.convertCharSequence(vf, ByteString.of("\"aaa\"@en-us")));
		assertEquals(Values.literal("aaa", Values.iri("http://example.org/#type")),
				RDFStreamUtils.convertCharSequence(vf, ByteString.of("\"aaa\"^^<http://example.org/#type>")));

	}
}

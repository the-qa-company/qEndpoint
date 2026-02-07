package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class RDFParserSimpleKeepBNodeTest {

	private static final String DATA = "_:a <http://example.com/p> _:b .\n";

	@Test
	public void keepBNodeFalseRelabelsBlankNodes() throws Exception {
		List<TripleString> relabeled = parse(false);
		assertEquals(1, relabeled.size());
		String subject = relabeled.get(0).getSubject().toString();
		String object = relabeled.get(0).getObject().toString();
		assertTrue(subject.startsWith("_:"));
		assertTrue(object.startsWith("_:"));
		assertNotEquals("_:a", subject);
		assertNotEquals("_:b", object);
	}

	@Test
	public void keepBNodeTruePreservesBlankNodes() throws Exception {
		List<TripleString> preserved = parse(true);
		assertEquals(1, preserved.size());
		assertEquals("_:a", preserved.get(0).getSubject().toString());
		assertEquals("_:b", preserved.get(0).getObject().toString());
	}

	private static List<TripleString> parse(boolean keepBNode) throws ParserException {
		RDFParserCallback parser = new RDFParserSimple();
		List<TripleString> triples = new ArrayList<>();
		ByteArrayInputStream input = new ByteArrayInputStream(DATA.getBytes(StandardCharsets.UTF_8));
		parser.doParse(input, "http://example.com/", RDFNotation.NTRIPLES, keepBNode,
				(triple, pos) -> triples.add(new TripleString(triple)));
		return triples;
	}
}

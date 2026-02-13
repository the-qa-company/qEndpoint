package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RDFParserSimpleJenaCompatTest {
	private static final Logger log = LoggerFactory.getLogger(RDFParserSimpleJenaCompatTest.class);
	private static final String BASE_URI = "http://www.rdfhdt.org";
	private static final String HEADER_TRIPLE = "<uri> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/HDT/hdt#Dataset> .\n";

	@Test
	@Ignore("Jena does not respect base URI for N-Triples parsing")
	public void jenaRelativeIriMatchesSimple() throws Exception {
		List<TripleString> jenaTriples = parseWithJena(HEADER_TRIPLE);
		logTriples("Jena", jenaTriples);
		Assert.assertEquals(1, jenaTriples.size());
		Assert.assertEquals("uri", jenaTriples.get(0).getSubject().toString());

		List<TripleString> simpleTriples = parseWithSimple(HEADER_TRIPLE);
		logTriples("Simple", simpleTriples);
		Assert.assertEquals(jenaTriples, simpleTriples);
	}

	private static List<TripleString> parseWithJena(String data) throws ParserException {
		RDFParserRIOT parser = new RDFParserRIOT();
		return parseWith(parser, data);
	}

	private static List<TripleString> parseWithSimple(String data) throws ParserException {
		RDFParserSimple parser = new RDFParserSimple();
		return parseWith(parser, data);
	}

	private static List<TripleString> parseWith(RDFParserRIOT parser, String data) throws ParserException {
		List<TripleString> triples = new ArrayList<>();
		ByteArrayInputStream input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
		parser.doParse(input, BASE_URI, RDFNotation.NTRIPLES, true,
				(triple, pos) -> triples.add(new TripleString(triple)));
		return triples;
	}

	private static List<TripleString> parseWith(RDFParserSimple parser, String data) throws ParserException {
		List<TripleString> triples = new ArrayList<>();
		ByteArrayInputStream input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
		parser.doParse(input, BASE_URI, RDFNotation.NTRIPLES, true,
				(triple, pos) -> triples.add(new TripleString(triple)));
		return triples;
	}

	private static void logTriples(String label, List<TripleString> triples) {
		for (TripleString triple : triples) {
			log.info("{} parsed triple: {}", label, triple);
			System.out.println(label + " parsed triple: " + triple);
		}
	}
}

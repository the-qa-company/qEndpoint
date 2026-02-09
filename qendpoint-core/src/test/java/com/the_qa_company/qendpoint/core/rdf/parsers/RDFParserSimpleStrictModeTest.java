package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.StreamRDFLib;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RDFParserSimpleStrictModeTest {
	private static final String BASE_URI = "http://www.rdfhdt.org";
	private static final String RELATIVE_IRI_DATA = "<rel> <http://example.org/p> <http://example.org/o> .\n";
	private static final String LEXICAL_MARKER_DATA = "<http://example.org/s> <http://example.org/p> \"value ^^< not-a-datatype >\" .\n";
	private static final String LEXICAL_RELATIVE_DATA = "<http://example.org/s> <http://example.org/p> \"value ^^<rel>\" .\n";

	@Test
	public void strictModeMatchesJenaForRelativeIri() throws Exception {
		assertJenaStrictRejects(RELATIVE_IRI_DATA);

		RDFParserSimple strictParser = newStrictParser();
		assertParseFails(strictParser, RELATIVE_IRI_DATA);

		RDFParserSimple lenientParser = new RDFParserSimple();
		assertParseSucceeds(lenientParser, RELATIVE_IRI_DATA);
	}

	@Test
	public void strictModeIgnoresDatatypeMarkerInsideLexicalForm() throws Exception {
		assertJenaStrictAccepts(LEXICAL_MARKER_DATA);

		RDFParserSimple strictParser = newStrictParser();
		assertParseSucceeds(strictParser, LEXICAL_MARKER_DATA);
	}

	@Test
	public void lenientModeDoesNotRewriteDatatypeMarkerInsideLexicalForm() throws Exception {
		RDFParserSimple lenientParser = new RDFParserSimple();

		List<TripleString> triples = parseWith(lenientParser, LEXICAL_RELATIVE_DATA);
		Assert.assertEquals(1, triples.size());
		Assert.assertEquals("\"value ^^<rel>\"", triples.get(0).getObject().toString());
	}

	private static RDFParserSimple newStrictParser() throws Exception {
		Constructor<RDFParserSimple> ctor = RDFParserSimple.class.getConstructor(boolean.class);
		return ctor.newInstance(true);
	}

	private static void assertJenaStrictAccepts(String data) {
		try (InputStream input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))) {
			RDFParser.source(input).base(BASE_URI).lang(Lang.NTRIPLES).strict(true).parse(StreamRDFLib.sinkNull());
		} catch (RiotException e) {
			Assert.fail("Expected Jena strict parsing to accept literal with ^^< inside lexical form");
		} catch (Exception e) {
			throw new AssertionError("Unexpected exception from Jena strict parser", e);
		}
	}

	private static void assertJenaStrictRejects(String data) {
		try (InputStream input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))) {
			RDFParser.source(input).base(BASE_URI).lang(Lang.NTRIPLES).strict(true).parse(StreamRDFLib.sinkNull());
			Assert.fail("Expected Jena strict parsing to reject relative IRIs");
		} catch (RiotException expected) {
			// expected
		} catch (Exception e) {
			throw new AssertionError("Unexpected exception from Jena strict parser", e);
		}
	}

	private static void assertParseFails(RDFParserSimple parser, String data) throws Exception {
		try (InputStream input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))) {
			parser.doParse(input, BASE_URI, RDFNotation.NTRIPLES, true, (triple, pos) -> {});
			Assert.fail("Expected strict parsing to reject relative IRIs");
		} catch (ParserException expected) {
			// expected
		}
	}

	private static void assertParseSucceeds(RDFParserSimple parser, String data) throws Exception {
		try (InputStream input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))) {
			parser.doParse(input, BASE_URI, RDFNotation.NTRIPLES, true, (triple, pos) -> {});
		}
	}

	private static List<TripleString> parseWith(RDFParserSimple parser, String data) throws ParserException {
		List<TripleString> triples = new ArrayList<>();
		ByteArrayInputStream input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
		parser.doParse(input, BASE_URI, RDFNotation.NTRIPLES, true,
				(triple, pos) -> triples.add(new TripleString(triple)));
		return triples;
	}
}

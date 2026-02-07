package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class RDFParserSimpleJenaCocktailsTest {
	private static final Logger log = LoggerFactory.getLogger(RDFParserSimpleJenaCocktailsTest.class);
	private static final String BASE_URI = "http://example.org/";
	private static final String RESOURCE_PATH = "qendpoint-backend/src/test/resources/cocktails.nt";

	@Test
	public void cocktailsMatchesJena() throws Exception {
		Path file = locateCocktails();
		List<TripleString> jenaTriples = parseWithJena(file);
		List<TripleString> simpleTriples = parseWithSimple(file);

		log.info("Jena triples: {}", jenaTriples.size());
		log.info("Simple triples: {}", simpleTriples.size());
		logSample("Jena", jenaTriples);
		logSample("Simple", simpleTriples);

		Assert.assertEquals("Triple counts differ", jenaTriples.size(), simpleTriples.size());
		assertSameTriples(jenaTriples, simpleTriples);
	}

	private static Path locateCocktails() {
		Path direct = Path.of(RESOURCE_PATH);
		if (Files.exists(direct)) {
			return direct;
		}
		Path fallback = Path.of("..", RESOURCE_PATH).normalize();
		if (Files.exists(fallback)) {
			return fallback;
		}
		throw new IllegalStateException("Missing resource: " + RESOURCE_PATH);
	}

	private static List<TripleString> parseWithJena(Path file) throws Exception {
		return parseWith(new RDFParserRIOT(), file);
	}

	private static List<TripleString> parseWithSimple(Path file) throws Exception {
		return parseWith(new RDFParserSimple(), file);
	}

	private static List<TripleString> parseWith(RDFParserRIOT parser, Path file) throws Exception {
		List<TripleString> triples = new ArrayList<>();
		try (InputStream input = Files.newInputStream(file)) {
			parser.doParse(input, BASE_URI, RDFNotation.NTRIPLES, true,
					(triple, pos) -> triples.add(new TripleString(triple)));
		}
		return triples;
	}

	private static List<TripleString> parseWith(RDFParserSimple parser, Path file) throws Exception {
		List<TripleString> triples = new ArrayList<>();
		try (InputStream input = Files.newInputStream(file)) {
			parser.doParse(input, BASE_URI, RDFNotation.NTRIPLES, true,
					(triple, pos) -> triples.add(new TripleString(triple)));
		}
		return triples;
	}

	private static void assertSameTriples(List<TripleString> expected, List<TripleString> actual) {
		int size = expected.size();
		for (int i = 0; i < size; i++) {
			TripleString left = expected.get(i);
			TripleString right = actual.get(i);
			if (!left.equals(right)) {
				Assert.fail("Triple mismatch at index " + i + "\nJena: " + left + "\nSimple: " + right);
			}
		}
	}

	private static void logSample(String label, List<TripleString> triples) {
		int limit = Math.min(5, triples.size());
		for (int i = 0; i < limit; i++) {
			log.info("{} sample {}: {}", label, i + 1, triples.get(i));
		}
	}
}

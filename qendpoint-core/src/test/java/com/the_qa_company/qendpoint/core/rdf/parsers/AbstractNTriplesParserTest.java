package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.Statements;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public abstract class AbstractNTriplesParserTest {

	protected abstract RDFParserCallback createParser();

	/** Test parsing of escaped unicode characters in IRIs. */
	@Test
	public void testIriUnescape() throws Exception {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, stream);
		writer.startRDF();
		IRI n = Values.iri("http://xÃ");
		writer.handleStatement(Statements.statement(n, n, n, null));
		writer.endRDF();

		String input = stream.toString(ByteStringUtil.STRING_ENCODING);

		List<TripleString> triples = parse(input, 1);

		TripleString triple = triples.get(0);
		assertEquals(n.stringValue(), triple.getSubject());
		assertEquals(n.stringValue(), triple.getPredicate());
		assertEquals(n.stringValue(), triple.getObject());
	}

	private List<TripleString> parse(String ntriples, int expectedCount) throws ParserException {
		InputStream in = new ByteArrayInputStream(ntriples.getBytes(UTF_8));

		final List<TripleString> triples = new ArrayList<>();
		createParser().doParse(in, "http://example.com#", RDFNotation.NTRIPLES, false,
				(triple, pos) -> triples.add(new TripleString(triple)));

		assertEquals(expectedCount, triples.size());
		return triples;
	}
}

package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import org.apache.jena.atlas.io.StringWriterI;
import org.apache.jena.atlas.lib.CharSpace;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public abstract class AbstractNTriplesParserTest {

	private interface Producer {
		void writeTo(StreamRDF out);
	}

	protected abstract RDFParserCallback createParser();

	/** Test parsing of escaped unicode characters in IRIs. */
	@Test
	public void testIriUnescape() throws Exception {
		final Node n = NodeFactory.createURI("x\u00c3");

		String input = format(CharSpace.ASCII, new Producer() {
			@Override
			public void writeTo(StreamRDF out) {
				out.triple(new Triple(n, n, n));
			}
		});

		List<TripleString> triples = parse(input, 1);

		TripleString triple = triples.get(0);
		assertEquals(JenaNodeFormatter.format(n), triple.getSubject());
		assertEquals(JenaNodeFormatter.format(n), triple.getPredicate());
		assertEquals(JenaNodeFormatter.format(n), triple.getObject());
	}

	@Test
	public void testStringUnescape() throws Exception {
		final Node s = NodeFactory.createURI("x");
		final Node p = NodeFactory.createURI("y");
		final Node o1 = NodeFactory.createLiteral("abc\u00c3", "en");
		final Node o2 = NodeFactory.createLiteral(JenaNodeFormatter.format(o1));
		final Node o3 = NodeFactory.createLiteral(JenaNodeFormatter.format(o2));

		String input = format(CharSpace.ASCII, new Producer() {
			@Override
			public void writeTo(StreamRDF out) {
				out.triple(new Triple(s, p, o1));
				out.triple(new Triple(s, p, o2));
				out.triple(new Triple(s, p, o3));
			}
		});

		List<TripleString> triples = parse(input, 3);

		assertEquals(JenaNodeFormatter.format(o1), triples.get(0).getObject());
		assertEquals(JenaNodeFormatter.format(o2), triples.get(1).getObject());
		assertEquals(JenaNodeFormatter.format(o3), triples.get(2).getObject());
	}

	private String format(CharSpace charSpace, Producer producer) {
		StringWriterI buf = new StringWriterI();
		StreamRDF out = StreamRDFLib.writer(buf, charSpace);
		out.start();
		producer.writeTo(out);
		out.finish();
		return buf.toString();
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

package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class UnicodeEscapeTest {

	@Test
	public void encodeTest() throws ParserException {
		String file = Objects.requireNonNull(UnicodeEscapeTest.class.getClassLoader().getResource("unicodeTest.nt"),
				"can't find file").getFile();

		RDFParserCallback factory = RDFParserFactory.getParserCallback(RDFNotation.NTRIPLES,
				HDTOptions.of(Map.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true")));
		RDFParserCallback factory2 = RDFParserFactory.getParserCallback(RDFNotation.NTRIPLES,
				HDTOptions.of(Map.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "false")));

		Set<TripleString> ts1 = Collections.synchronizedSet(new TreeSet<>(Comparator.comparing(t -> {
			try {
				return t.asNtriple().toString();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		})));
		Set<TripleString> ts2 = Collections.synchronizedSet(new TreeSet<>(Comparator.comparing(t -> {
			try {
				return t.asNtriple().toString();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		})));
		factory.doParse(file, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, true, (t, i) -> ts1.add(t.tripleToString()));
		factory2.doParse(file, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, true,
				(t, i) -> ts2.add(t.tripleToString()));

		Iterator<TripleString> it1 = ts1.iterator();
		Iterator<TripleString> it2 = ts2.iterator();

		HDTTestUtils.CoIterator<TripleString, TripleString> it = new HDTTestUtils.CoIterator<>(it1, it2);

		while (it.hasNext()) {
			HDTTestUtils.Tuple<TripleString, TripleString> e = it.next();
			System.out.println(e);
			assertEquals(e.t1, e.t2);
		}
	}

	@Test
	public void decodeTest() {
		assertEquals(new String(Character.toChars(0x0002dd76)), UnicodeEscape.unescapeString("\\U0002dd76"));

	}
}

package com.the_qa_company.qendpoint.core.hdt.impl;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@RunWith(Parameterized.class)
public class TempHDTImporterTest {
	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object> setup() {
		return Arrays.asList("one-pass", "two-pass");
	}

	private final HDTSpecification spec;

	public TempHDTImporterTest(String mode) {
		spec = new HDTSpecification();
		spec.set("loader.type", mode);
		spec.set("loader.bnode.seed", "1234567");
	}

	private String getFile(String f) {
		return Objects.requireNonNull(getClass().getClassLoader().getResource(f), "Can't find " + f).getFile();
	}

	@Test
	public void bNodeXTest() throws ParserException, IOException {
		HDTManager.generateHDT(getFile("importer/bnode_x.nt"), HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec, null)
				.close();
	}

	@Test
	public void bNodeZTest() throws ParserException, IOException {
		HDTManager.generateHDT(getFile("importer/bnode_z.nt"), HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec, null)
				.close();
	}

	private Iterator<TripleString> asIt(String file) throws ParserException {
		List<TripleString> triples = new ArrayList<>();
		RDFNotation notation = RDFNotation.guess(file);
		RDFParserCallback parser = RDFParserFactory.getParserCallback(notation);
		parser.doParse(file, HDTTestUtils.BASE_URI, notation, true, (triple, pos) -> {
			// force duplication of the triple string data
			triples.add(new TripleString(triple.getSubject().toString(), triple.getPredicate().toString(),
					triple.getObject().toString()));
		});
		return triples.iterator();
	}

	@Test
	public void bNodeXStreamTest() throws ParserException, IOException {
		HDTManager.generateHDT(asIt(getFile("importer/bnode_x.nt")), HDTTestUtils.BASE_URI, spec, null).close();
	}

	@Test
	public void bNodeZStreamTest() throws ParserException, IOException {
		HDTManager.generateHDT(asIt(getFile("importer/bnode_z.nt")), HDTTestUtils.BASE_URI, spec, null).close();
	}
}

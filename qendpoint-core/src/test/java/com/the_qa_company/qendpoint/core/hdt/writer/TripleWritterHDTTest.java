package com.the_qa_company.qendpoint.core.hdt.writer;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import org.junit.Before;
import org.junit.Test;

public class TripleWritterHDTTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() throws IOException, ParserException {
		File file = new File("out.hdt");
		file.deleteOnExit();

		final TripleWriterHDT wr = new TripleWriterHDT("http://example.org", new HDTSpecification(), file.toString(),
				false);

		RDFParserCallback pars = RDFParserFactory.getParserCallback(RDFNotation.NTRIPLES);
		pars.doParse("data/test.nt", "http://example.org", RDFNotation.NTRIPLES, false, (triple, pos) -> {
			try {
				wr.addTriple(triple);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
		wr.close();

		HDT hdt = HDTManager.loadHDT(file.toString());
		assertEquals("Successfully loaded HDT with same number of triples", 10, hdt.getTriples().getNumberOfElements());

		// Delete temp file
		Files.delete(file.toPath());
	}

}

package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

public class RDFParserRIOTTest extends AbstractNTriplesParserTest {

	@Override
	protected RDFParserCallback createParser() {
		return new RDFParserRIOT();
	}

	@Test(expected = ParserException.class)
	public void parallelFailTest() throws IOException, ParserException {
		RDFParserCallback parser = createParser();
		try (InputStream is = new BufferedInputStream(Files.newInputStream(createDataset(true)))) {
			parser.doParse(is, LargeFakeDataSetStreamSupplier.BASE_URI, RDFNotation.NTRIPLES, true, ((triple, pos) -> {
				// nothing
			}), true);
		}
	}

	@Test(expected = ParserException.class)
	public void singleFailTest() throws IOException, ParserException {
		RDFParserCallback parser = createParser();
		try (InputStream is = new BufferedInputStream(Files.newInputStream(createDataset(true)))) {
			parser.doParse(is, LargeFakeDataSetStreamSupplier.BASE_URI, RDFNotation.NTRIPLES, true, ((triple, pos) -> {
				// nothing
			}), false);
		}
	}
}

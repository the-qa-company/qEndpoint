package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

public class RDFParserHDTTest {

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void hdtTest() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFile("test.hdt").toPath();

		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(20, 34);

		HDT hdt = HDTManager.generateHDT(supplier.createTripleStringStream(), "http://example.org/#",
				new HDTSpecification(), null);
		hdt.saveToHDT(root.toAbsolutePath().toString(), null);

		supplier.reset();

		String filename = root.toAbsolutePath().toString();
		RDFNotation dir = RDFNotation.guess(filename);
		Assert.assertEquals(dir, RDFNotation.HDT);
		RDFParserCallback callback = dir.createCallback();
		Assert.assertTrue(callback instanceof RDFParserHDT);

		IteratorTripleString it = hdt.search("", "", "");

		callback.doParse(filename, "http://example.org/#", dir, true,
				(triple, pos) -> Assert.assertEquals(it.next(), triple));

		hdt.close();
	}
}

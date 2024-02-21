package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.tools.HDTVerify;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.store.Utility;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RDFStreamUtilsTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void convertTest() {
		SimpleValueFactory vf = SimpleValueFactory.getInstance();

		assertEquals(Values.iri("http://example.org/#me"),
				RDFStreamUtils.convertCharSequence(vf, ByteString.of("http://example.org/#me")));

		assertEquals(Values.iri("http://example.org/#me"),
				RDFStreamUtils.convertCharSequence(vf, ByteString.of("<http://example.org/#me>")));

		assertEquals(Values.bnode("coolnode"), RDFStreamUtils.convertCharSequence(vf, ByteString.of("_:coolnode")));

		assertEquals(Values.literal("aaa"), RDFStreamUtils.convertCharSequence(vf, ByteString.of("\"aaa\"")));

		assertEquals(Values.literal("aaa", "en-us"),
				RDFStreamUtils.convertCharSequence(vf, ByteString.of("\"aaa\"@en-us")));
		assertEquals(Values.literal("aaa", Values.iri("http://example.org/#type")),
				RDFStreamUtils.convertCharSequence(vf, ByteString.of("\"aaa\"^^<http://example.org/#type>")));

	}

	@Test
	public void streamTest() throws IOException, ParserException, NotFoundException {
		Path file = tempDir.newFile().toPath();
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
				.createSupplierWithMaxTriples(1000000, 42).withUnicode(true).withMaxLiteralSize(1)
				.withMaxElementSplit(10);
		try (OutputStream writer = new BufferedOutputStream(Files.newOutputStream(file))) {
			RDFWriter rdfWriter = Rio.createWriter(RDFFormat.TURTLE, writer);
			rdfWriter.startRDF();

			SimpleValueFactory vf = SimpleValueFactory.getInstance();
			Iterator<TripleString> sup = supplier.createTripleStringStream();
			while (sup.hasNext()) {
				TripleString ts = sup.next();
				Statement statement = RDFStreamUtils.convertStatement(vf, ts);
				try {
					rdfWriter.handleStatement(statement);
					writer.flush();
				} catch (Throwable t) {
					System.out.println(statement);
					System.out.println(Arrays.toString(statement.toString().getBytes(StandardCharsets.UTF_8)));
					throw t;
				}
			}

			rdfWriter.endRDF();
		}
		try {
			try (BufferedInputStream is = new BufferedInputStream(Files.newInputStream(file))) {
				Iterator<TripleString> it = RDFStreamUtils.readRDFStreamAsTripleStringIterator(is, RDFFormat.TURTLE,
						true);
				supplier.reset();
				Iterator<TripleString> it2 = supplier.createTripleStringStream();

				HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
						HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);
				try (HDT hdt1 = HDTManager.generateHDT(it, Utility.EXAMPLE_NAMESPACE, spec, ProgressListener.ignore());
						HDT hdt2 = HDTManager.generateHDT(it2, Utility.EXAMPLE_NAMESPACE, spec,
								ProgressListener.ignore())) {

					HDTVerify.checkHDT(hdt1, true, ProgressListener.ignore());
					HDTVerify.checkHDT(hdt2, true, ProgressListener.ignore());
					IteratorTripleString ait1 = hdt1.searchAll();
					IteratorTripleString ait2 = hdt2.searchAll();

					while (ait1.hasNext()) {
						TripleString ats1 = ait1.next();
						assertTrue(ait2.hasNext());
						TripleString ats2 = ait2.next();
						assertEquals(ats1, ats2);
					}
					assertFalse(ait2.hasNext());

					System.out.println(hdt1.getDictionary().getType());
					System.out.println(hdt1.getTriples().getNumberOfElements());
				}

			}

		} finally {
			Files.deleteIfExists(file);
		}
	}
}

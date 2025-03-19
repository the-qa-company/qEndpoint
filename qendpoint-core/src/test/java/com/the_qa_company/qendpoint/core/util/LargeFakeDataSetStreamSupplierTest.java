package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import org.spf4j.io.BufferedInputStream;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import com.the_qa_company.qendpoint.core.hdt.HDTManagerTest;
import com.the_qa_company.qendpoint.core.iterator.utils.CombinedIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LargeFakeDataSetStreamSupplierTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void streamTest() throws IOException {
		LargeFakeDataSetStreamSupplier triples = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(10, 10);
		Path f = tempDir.newFolder().toPath();
		Path testNt = f.resolve("test.nt");
		triples.createNTFile(testNt.toAbsolutePath().toString());
		triples.reset();

		Iterator<TripleString> it2 = triples.createTripleStringStream();
		try (InputStream is = Files.newInputStream(testNt)) {
			try (PipedCopyIterator<TripleString> it = RDFParserFactory.readAsIterator(
					RDFParserFactory.getParserCallback(RDFNotation.NTRIPLES), is, HDTTestUtils.BASE_URI, true,
					RDFNotation.NTRIPLES)) {
				it.forEachRemaining(s -> {
					assertTrue(it2.hasNext());
					assertEquals(it2.next(), s);
				});
				assertFalse(it.hasNext());
			}
		}
	}

	@Test
	public void sameTest() {
		long[] sizes = { 50, 25, 32, 10, 0, 12 };
		LargeFakeDataSetStreamSupplier s1 = LargeFakeDataSetStreamSupplier.createInfinite(34);
		LargeFakeDataSetStreamSupplier s2 = LargeFakeDataSetStreamSupplier.createInfinite(34);

		Iterator<TripleString> it1 = CombinedIterator.combine(LongStream.of(sizes)
				.mapToObj(s -> s1.withMaxTriples(s).createTripleStringStream()).collect(Collectors.toList()));

		Iterator<TripleString> it2 = s2.withMaxTriples(LongStream.of(sizes).sum()).createTripleStringStream();

		while (it2.hasNext()) {
			assertTrue(it1.hasNext());
			assertEquals(it2.next(), it1.next());
		}
		assertFalse(it1.hasNext());
	}

	@Test
	public void countTest() {
		long size = 42;
		Iterator<TripleString> it = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(size, 34)
				.createTripleStringStream();
		int count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(size, count);
	}

	@Test
	public void countTest2() {
		long size = 42;
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(size, 34);
		{
			Iterator<TripleString> it = supplier.createTripleStringStream();
			int count = 0;
			while (it.hasNext()) {
				it.next();
				count++;
			}
			assertEquals(size, count);
		}
		{
			Iterator<TripleString> it = supplier.createTripleStringStream();
			int count = 0;
			while (it.hasNext()) {
				it.next();
				count++;
			}
			assertEquals(size, count);
		}
		{
			Iterator<TripleString> it = supplier.createTripleStringStream();
			int count = 0;
			while (it.hasNext()) {
				it.next();
				count++;
			}
			assertEquals(size, count);
		}
	}

	@Test
	public void countTest3() {
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createInfinite(34);
		{
			long size = 42;
			Iterator<TripleString> it = supplier.withMaxTriples(size).createTripleStringStream();
			int count = 0;
			while (it.hasNext()) {
				it.next();
				count++;
			}
			assertEquals(size, count);
		}
		{
			long size = 24;
			Iterator<TripleString> it = supplier.withMaxTriples(size).createTripleStringStream();
			int count = 0;
			while (it.hasNext()) {
				it.next();
				count++;
			}
			assertEquals(size, count);
		}
		{
			long size = 35;
			Iterator<TripleString> it = supplier.withMaxTriples(size).createTripleStringStream();
			int count = 0;
			while (it.hasNext()) {
				it.next();
				count++;
			}
			assertEquals(size, count);
		}
	}

	@Test
	public void mergeTest() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();
		long size = 42;
		long seed = 54;
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxSize(size, seed);

		Path p12 = root.resolve("p12.nt");
		Path p12HDT = root.resolve("p12.hdt");
		Path p3 = root.resolve("p3.nt");
		Path p3HDT = root.resolve("p3.hdt");

		try (BufferedWriter w = Files.newBufferedWriter(p12)) {
			supplier.createNTFile(w);
			supplier.createNTFile(w);
		}

		LargeFakeDataSetStreamSupplier supplier2 = LargeFakeDataSetStreamSupplier.createSupplierWithMaxSize(size * 2,
				seed);

		supplier2.createNTFile(p3);

		RDFParserCallback parser = RDFParserFactory.getParserCallback(RDFNotation.NTRIPLES,
				HDTOptions.of(Map.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true")));
		try {
			try (InputStream stream = new BufferedInputStream(Files.newInputStream(p12));
					InputStream stream2 = new BufferedInputStream(Files.newInputStream(p3));
					PipedCopyIterator<TripleString> it1 = RDFParserFactory.readAsIterator(parser, stream, "http://w",
							true, RDFNotation.NTRIPLES);
					PipedCopyIterator<TripleString> it2 = RDFParserFactory.readAsIterator(parser, stream2, "http://w",
							true, RDFNotation.NTRIPLES)) {
				while (it1.hasNext()) {
					assertTrue(it2.hasNext());
					assertEquals(it1.next(), it2.next());
				}
				assertFalse(it2.hasNext());
			}
			try (HDT exceptedHdt = HDTManager.generateHDT(p12.toAbsolutePath().toString(), "http://w",
					RDFNotation.NTRIPLES, new HDTSpecification(), null);
					HDT actualHDT = HDTManager.generateHDT(p3.toAbsolutePath().toString(), "http://w",
							RDFNotation.NTRIPLES, new HDTSpecification(), null)) {
				exceptedHdt.saveToHDT(p12HDT.toAbsolutePath().toString(), null);
				actualHDT.saveToHDT(p3HDT.toAbsolutePath().toString(), null);
			}

			try (HDT p1HDTLoad = HDTManager.mapHDT(p12HDT.toAbsolutePath().toString());
					HDT p3HDTLoad = HDTManager.mapHDT(p3HDT.toAbsolutePath().toString())) {
				HDTManagerTest.HDTManagerTestBase.assertEqualsHDT(p1HDTLoad, p3HDTLoad);
				supplier2.reset();
				try (HDT actual = supplier2.createFakeHDT(new HDTSpecification())) {
					HDTManagerTest.HDTManagerTestBase.checkHDTConsistency(actual);
					HDTManagerTest.HDTManagerTestBase.assertEqualsHDT(p3HDTLoad, actual);
				}
			}
		} finally {
			try {
				Files.deleteIfExists(p12);
			} finally {
				Files.deleteIfExists(p3);
			}
		}
	}
}

package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.header.HeaderUtil;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import org.apache.commons.io.file.PathUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RDFParserDirTest {
	private static final boolean LOG_TIME = false;

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void dirTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();
		Files.createDirectories(root);

		Path testDir1 = root.resolve("testDir1");
		Path testDir2 = root.resolve("testDir2");
		Path testDir3 = root.resolve("testDir3");
		Path testDir4 = testDir3.resolve("testDir4");

		Files.createDirectories(testDir1);
		Files.createDirectories(testDir2);
		Files.createDirectories(testDir3);
		Files.createDirectories(testDir4);

		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(20, 34);

		supplier.createNTFile(root.resolve("test.nt").toAbsolutePath().toString());
		supplier.createNTFile(testDir1.resolve("test1.nt").toAbsolutePath().toString());
		supplier.createNTFile(testDir2.resolve("test21.nt").toAbsolutePath().toString());
		supplier.createNTFile(testDir2.resolve("test22.nt").toAbsolutePath().toString());
		supplier.createNTFile(testDir3.resolve("test31.nt").toAbsolutePath().toString());
		supplier.createNTFile(testDir3.resolve("test32.nt").toAbsolutePath().toString());

		Files.writeString(testDir2.resolve("thing.txt"), "Not parsable RDF DATA");
		Files.writeString(root.resolve("thing.py"), "print('Not parsable RDF DATA')");
		Files.writeString(testDir4.resolve("thing.sh"), "echo \"Not Parsable RDF data\"");

		supplier.reset();

		List<TripleString> excepted = new ArrayList<>();
		// 6 for the 6 files
		for (int i = 0; i < 6; i++) {
			Iterator<TripleString> it = supplier.createTripleStringStream();
			while (it.hasNext()) {
				TripleString ts = it.next();
				TripleString e = new TripleString(HeaderUtil.cleanURI(ts.getSubject().toString()),
						HeaderUtil.cleanURI(ts.getPredicate().toString()),
						HeaderUtil.cleanURI(ts.getObject().toString()));
				excepted.add(e);
			}
		}

		String filename = root.toAbsolutePath().toString();
		RDFNotation dir = RDFNotation.guess(filename);
		Assert.assertEquals(dir, RDFNotation.DIR);
		RDFParserCallback callback = RDFParserFactory.getParserCallback(dir);
		assertTrue(callback instanceof RDFParserDir);

		callback.doParse(filename, "http://example.org/#", dir, true,
				(triple, pos) -> assertTrue("triple " + triple + " wasn't excepted", excepted.remove(triple)), true);
	}

	@Test
	public void asyncTest() throws IOException, ParserException {
		// create fake dataset
		int maxThread = 20;
		int filePerDir = maxThread * 2 / 3;
		int files = maxThread * 10;
		long triplePerFile = 1000;

		Path root = tempDir.newFolder().toPath();
		Files.createDirectories(root);
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
				.createSupplierWithMaxTriples(triplePerFile, 86);
		try {
			List<Path> allFiles = new ArrayList<>();
			for (int i = 0; i < files; i++) {
				Path location = root;
				int j = i;
				while (j > filePerDir) {
					location = location.resolve("sub" + (j % filePerDir));
					j /= filePerDir;
				}
				Files.createDirectories(location);
				Path tmpFile = location.resolve("test_" + i + ".nt");
				supplier.createNTFile(tmpFile);
				allFiles.add(tmpFile);
			}

			StopWatch time = new StopWatch();
			RDFContainer containerSimple = new RDFContainer();
			{
				for (Path path : allFiles) {
					RDFParserCallback parser = RDFParserFactory.getParserCallback(RDFNotation.NTRIPLES);
					parser.doParse(path.toAbsolutePath().toString(), HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, true,
							containerSimple, true);
				}
			}
			time.stop();
			if (LOG_TIME) {
				System.out.println("simple parsing: " + time);
			}
			time.reset();

			RDFContainer containerAsync = new RDFContainer();
			{
				RDFNotation notation = RDFNotation.guess(root);
				assertEquals(notation, RDFNotation.DIR);
				RDFParserCallback parser = RDFParserFactory.getParserCallback(notation,
						HDTOptions.of(Map.of(HDTOptionsKeys.ASYNC_DIR_PARSER_KEY, "" + maxThread)));
				assertTrue(parser instanceof RDFParserDir);
				assertEquals(maxThread, ((RDFParserDir) parser).async);

				parser.doParse(root.toAbsolutePath().toString(), HDTTestUtils.BASE_URI, notation, true, containerAsync,
						true);
			}
			time.stop();
			if (LOG_TIME) {
				System.out.println("async parsing: " + time);
			}
			time.reset();

			assertEquals("Triples aren't matching with async version!", containerSimple.getTriples(),
					containerAsync.getTriples());

			RDFContainer containerSync = new RDFContainer();
			{
				RDFNotation notation = RDFNotation.guess(root);
				assertEquals(notation, RDFNotation.DIR);
				RDFParserCallback parser = RDFParserFactory.getParserCallback(notation, HDTOptions.EMPTY);
				assertTrue(parser instanceof RDFParserDir);
				assertEquals(1, ((RDFParserDir) parser).async);

				parser.doParse(root.toAbsolutePath().toString(), HDTTestUtils.BASE_URI, notation, true, containerSync,
						true);
			}

			time.stop();
			if (LOG_TIME) {
				System.out.println("sync parsing: " + time);
			}
			time.reset();

			assertEquals("Triples aren't matching with sync version!", containerSimple.getTriples(),
					containerSync.getTriples());
		} finally {
			PathUtils.deleteDirectory(root);
		}

	}
}

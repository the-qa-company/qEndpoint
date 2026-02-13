package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.quad.QuadString;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NQuadsChunkedDiskImportTest {
	private static final String NQUADS_WITH_BNODES = String.join("\n", "_:a <http://ex/p1> _:b <http://ex/g1> .",
			"_:a <http://ex/p2> \"lit\" <http://ex/g1> .", "<http://ex/s1> <http://ex/p3> _:b <http://ex/g1> .",
			"_:c <http://ex/p4> <http://ex/o1> _:g .", "_:c <http://ex/p5> _:b _:g .", "");
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void generateDiskWithSimpleParserUsesPullChunking() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();
		Path nq = root.resolve("data.nq");

		Files.writeString(nq, "<http://ex/s> <http://ex/p> \"o\" <http://ex/g> .\n"
				+ "<http://ex/s2> <http://ex/p2> <http://ex/o2> <http://ex/g2> .\n");

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true", HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY,
				root.resolve("work").toAbsolutePath().toString(), HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY,
				"2", HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, "1");

		try (InputStream input = Files.newInputStream(nq);
				HDT hdt = HDTManager.generateHDTDisk(input, "http://ex/", RDFNotation.NQUAD, spec, null)) {
			assertEquals(2, hdt.getTriples().getNumberOfElements());
		}
	}

	@Test
	public void chunkedNQuadsKeepsBlankNodeIdsAcrossChunks() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();
		Path nq = root.resolve("bnodes.nq");
		Files.writeString(nq, NQUADS_WITH_BNODES);

		Map<String, QuadString> byPredicate = readChunkedQuads(root, nq, true);

		assertEquals("_:a", byPredicate.get("http://ex/p1").getSubject().toString());
		assertEquals("_:b", byPredicate.get("http://ex/p1").getObject().toString());
		assertEquals("http://ex/g1", byPredicate.get("http://ex/p1").getGraph().toString());

		assertEquals("_:a", byPredicate.get("http://ex/p2").getSubject().toString());
		assertEquals("\"lit\"", byPredicate.get("http://ex/p2").getObject().toString());
		assertEquals("http://ex/g1", byPredicate.get("http://ex/p2").getGraph().toString());

		assertEquals("http://ex/s1", byPredicate.get("http://ex/p3").getSubject().toString());
		assertEquals("_:b", byPredicate.get("http://ex/p3").getObject().toString());
		assertEquals("http://ex/g1", byPredicate.get("http://ex/p3").getGraph().toString());

		assertEquals("_:c", byPredicate.get("http://ex/p4").getSubject().toString());
		assertEquals("http://ex/o1", byPredicate.get("http://ex/p4").getObject().toString());
		assertEquals("_:g", byPredicate.get("http://ex/p4").getGraph().toString());

		assertEquals("_:c", byPredicate.get("http://ex/p5").getSubject().toString());
		assertEquals("_:b", byPredicate.get("http://ex/p5").getObject().toString());
		assertEquals("_:g", byPredicate.get("http://ex/p5").getGraph().toString());
	}

	@Test
	public void chunkedNQuadsMintsBlankNodeIdsConsistentlyAcrossChunks()
			throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();
		Path nq = root.resolve("bnodes.nq");
		Files.writeString(nq, NQUADS_WITH_BNODES);

		Map<String, QuadString> byPredicate = readChunkedQuads(root, nq, false);

		QuadString p1 = byPredicate.get("http://ex/p1");
		QuadString p2 = byPredicate.get("http://ex/p2");
		QuadString p3 = byPredicate.get("http://ex/p3");
		QuadString p4 = byPredicate.get("http://ex/p4");
		QuadString p5 = byPredicate.get("http://ex/p5");

		String a = p1.getSubject().toString();
		String b = p1.getObject().toString();
		String c = p4.getSubject().toString();
		String g = p4.getGraph().toString();

		assertTrue(a.startsWith("_:"));
		assertTrue(b.startsWith("_:"));
		assertTrue(c.startsWith("_:"));
		assertTrue(g.startsWith("_:"));

		assertEquals(a, p2.getSubject().toString());
		assertEquals(b, p3.getObject().toString());
		assertEquals(b, p5.getObject().toString());
		assertEquals(c, p5.getSubject().toString());
		assertEquals(g, p5.getGraph().toString());

		assertNotEquals("_:a", a);
		assertNotEquals("_:b", b);
		assertNotEquals("_:c", c);
		assertNotEquals("_:g", g);

		assertNotEquals(a, b);
		assertNotEquals(a, c);
		assertNotEquals(b, c);
		assertNotEquals(c, g);
	}

	private static Map<String, QuadString> readChunkedQuads(Path root, Path nq, boolean keepBNodes)
			throws IOException, ParserException, NotFoundException {
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true", HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY,
				root.resolve("work").toAbsolutePath().toString(), HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY,
				"2", HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, "1", HDTOptionsKeys.PARSER_KEEP_BNODE_KEY,
				Boolean.toString(keepBNodes));

		try (HDT hdt = HDTManager.generateHDTDisk(nq.toString(), "http://ex/", RDFNotation.NQUAD, spec, null)) {
			Map<String, QuadString> byPredicate = new HashMap<>();
			IteratorTripleString it = hdt.search("", "", "");
			while (it.hasNext()) {
				QuadString quad = new QuadString(it.next());
				byPredicate.put(quad.getPredicate().toString(), quad);
			}

			assertEquals(5, byPredicate.size());
			assertNotNull(byPredicate.get("http://ex/p1"));
			assertNotNull(byPredicate.get("http://ex/p2"));
			assertNotNull(byPredicate.get("http://ex/p3"));
			assertNotNull(byPredicate.get("http://ex/p4"));
			assertNotNull(byPredicate.get("http://ex/p5"));
			return byPredicate;
		}
	}
}

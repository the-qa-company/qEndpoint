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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class RdfFormatBlankNodeDiskImportTest {
	private static final String BASE_URI = "http://ex/";
	private static final String P1 = "http://ex/p1";
	private static final String P2 = "http://ex/p2";
	private static final String P3 = "http://ex/p3";
	private static final String P4 = "http://ex/p4";
	private static final String P5 = "http://ex/p5";

	private static final String TRIPLES_WITH_BNODES = String.join("\n", "_:a <http://ex/p1> _:b .",
			"_:a <http://ex/p2> \"lit\" .", "<http://ex/s1> <http://ex/p3> _:b .", "");
	private static final String NQUADS_WITH_BNODES = String.join("\n", "_:a <http://ex/p1> _:b <http://ex/g1> .",
			"_:a <http://ex/p2> \"lit\" <http://ex/g1> .", "<http://ex/s1> <http://ex/p3> _:b <http://ex/g1> .",
			"_:c <http://ex/p4> <http://ex/o1> _:g .", "_:c <http://ex/p5> _:b _:g .", "");
	private static final String RDFXML_WITH_BNODES = String.join("\n", "<?xml version=\"1.0\"?>",
			"<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:ex=\"http://ex/\">",
			"  <rdf:Description rdf:nodeID=\"a\">", "    <ex:p1 rdf:nodeID=\"b\"/>", "    <ex:p2>lit</ex:p2>",
			"  </rdf:Description>", "  <rdf:Description rdf:about=\"http://ex/s1\">", "    <ex:p3 rdf:nodeID=\"b\"/>",
			"  </rdf:Description>", "</rdf:RDF>", "");
	private static final String TRIG_WITH_BNODES = String.join("\n", "<http://ex/g1> {", "  _:a <http://ex/p1> _:b .",
			"  _:a <http://ex/p2> \"lit\" .", "  <http://ex/s1> <http://ex/p3> _:b .", "}", "_:g {",
			"  _:c <http://ex/p4> <http://ex/o1> .", "  _:c <http://ex/p5> _:b .", "}", "");
	private static final String TRIX_WITH_BNODES = String.join("\n", "<?xml version=\"1.0\"?>",
			"<trix xmlns=\"http://www.w3.org/2004/03/trix/trix-1/\">", "  <graph>", "    <uri>http://ex/g1</uri>",
			"    <triple>", "      <id>a</id>", "      <uri>http://ex/p1</uri>", "      <id>b</id>", "    </triple>",
			"    <triple>", "      <id>a</id>", "      <uri>http://ex/p2</uri>",
			"      <plainLiteral>lit</plainLiteral>", "    </triple>", "    <triple>", "      <uri>http://ex/s1</uri>",
			"      <uri>http://ex/p3</uri>", "      <id>b</id>", "    </triple>", "  </graph>", "  <graph>",
			"    <id>g</id>", "    <triple>", "      <id>c</id>", "      <uri>http://ex/p4</uri>",
			"      <uri>http://ex/o1</uri>", "    </triple>", "    <triple>", "      <id>c</id>",
			"      <uri>http://ex/p5</uri>", "      <id>b</id>", "    </triple>", "  </graph>", "</trix>", "");

	@Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(
				new Object[][] { { "NTRIPLES", RDFNotation.NTRIPLES, "data.nt", TRIPLES_WITH_BNODES, false, true },
						{ "NQUAD", RDFNotation.NQUAD, "data.nq", NQUADS_WITH_BNODES, true, true },
						{ "TURTLE", RDFNotation.TURTLE, "data.ttl", TRIPLES_WITH_BNODES, false, true },
						{ "N3", RDFNotation.N3, "data.n3", TRIPLES_WITH_BNODES, false, true },
						{ "TRIG", RDFNotation.TRIG, "data.trig", TRIG_WITH_BNODES, true, true },
						{ "TRIX", RDFNotation.TRIX, "data.trix", TRIX_WITH_BNODES, true, true } });
	}

	@Parameter(0)
	public String name;

	@Parameter(1)
	public RDFNotation notation;

	@Parameter(2)
	public String filename;

	@Parameter(3)
	public String payload;

	@Parameter(4)
	public boolean hasGraph;

	@Parameter(5)
	public boolean expectsOriginalLabels;

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void keepsBlankNodeIdsWhenRequested() throws IOException, ParserException, NotFoundException {
		Map<String, QuadString> byPredicate = readQuads(true);
		if (hasGraph) {
			assertKeepsBlankNodesWithGraph(byPredicate, expectsOriginalLabels);
		} else {
			assertKeepsBlankNodesWithoutGraph(byPredicate, expectsOriginalLabels);
		}
	}

	@Test
	public void mintsBlankNodeIdsWhenRequested() throws IOException, ParserException, NotFoundException {
		Map<String, QuadString> byPredicate = readQuads(false);
		if (hasGraph) {
			assertMintsBlankNodesWithGraph(byPredicate);
		} else {
			assertMintsBlankNodesWithoutGraph(byPredicate);
		}
	}

	@Test
	public void readdingDataKeepsCountWhenBNodesRetained() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();
		Path data = root.resolve(filename);
		Files.writeString(data, payload);

		long baseCount = hasGraph ? 5L : 3L;
		List<Path> hdts = createHdtFiles(root, data, true, 3);

		for (int i = 1; i <= hdts.size(); i++) {
			try (HDT merged = HDTManager.catHDTPath(hdts.subList(0, i), HDTOptions.of(), null)) {
				assertEquals(baseCount, merged.getTriples().getNumberOfElements());
			}
		}
	}

	@Test
	public void readdingDataGrowsCountWhenBNodesMinted() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();
		Path data = root.resolve(filename);
		Files.writeString(data, payload);

		long baseCount = hasGraph ? 5L : 3L;
		List<Path> hdts = createHdtFiles(root, data, false, 3);

		for (int i = 1; i <= hdts.size(); i++) {
			try (HDT merged = HDTManager.catHDTPath(hdts.subList(0, i), HDTOptions.of(), null)) {
				assertEquals(baseCount * i, merged.getTriples().getNumberOfElements());
			}
		}
	}

	private Map<String, QuadString> readQuads(boolean keepBNodes)
			throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();
		Path data = root.resolve(filename);
		Files.writeString(data, payload);

		HDTOptions spec = buildOptions(root.resolve("work"), keepBNodes);
		try (HDT hdt = HDTManager.generateHDTDisk(data.toString(), BASE_URI, notation, spec, null)) {
			Map<String, QuadString> byPredicate = new HashMap<>();
			IteratorTripleString it = hdt.search("", "", "");
			while (it.hasNext()) {
				QuadString quad = new QuadString(it.next());
				byPredicate.put(quad.getPredicate().toString(), quad);
			}

			if (hasGraph) {
				assertEquals(5, byPredicate.size());
				assertNotNull(byPredicate.get(P4));
				assertNotNull(byPredicate.get(P5));
			} else {
				assertEquals(3, byPredicate.size());
			}
			assertNotNull(byPredicate.get(P1));
			assertNotNull(byPredicate.get(P2));
			assertNotNull(byPredicate.get(P3));
			return byPredicate;
		}
	}

	private List<Path> createHdtFiles(Path root, Path data, boolean keepBNodes, int count)
			throws IOException, ParserException {
		List<Path> hdts = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			Path hdtFile = root.resolve("data-" + i + ".hdt");
			HDTOptions spec = buildOptions(root.resolve("work-" + i), keepBNodes);
			try (HDT hdt = HDTManager.generateHDTDisk(data.toString(), BASE_URI, notation, spec, null)) {
				hdt.saveToHDT(hdtFile);
			}
			hdts.add(hdtFile);
		}
		return hdts;
	}

	private HDTOptions buildOptions(Path workDir, boolean keepBNodes) {
		if (hasGraph) {
			return HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true", HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY,
					workDir.toAbsolutePath().toString(), HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "2",
					HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, "1", HDTOptionsKeys.PARSER_KEEP_BNODE_KEY,
					Boolean.toString(keepBNodes));
		}
		return HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true", HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY,
				workDir.toAbsolutePath().toString(), HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "2",
				HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, "1", HDTOptionsKeys.PARSER_KEEP_BNODE_KEY,
				Boolean.toString(keepBNodes));
	}

	private void assertKeepsBlankNodesWithoutGraph(Map<String, QuadString> byPredicate, boolean expectOriginalLabels) {
		QuadString p1 = byPredicate.get(P1);
		QuadString p2 = byPredicate.get(P2);
		QuadString p3 = byPredicate.get(P3);

		String a = p1.getSubject().toString();
		String b = p1.getObject().toString();

		assertTrue(a.startsWith("_:"));
		assertTrue(b.startsWith("_:"));
		assertEquals(a, p2.getSubject().toString());
		assertEquals(b, p3.getObject().toString());

		if (expectOriginalLabels) {
			assertEquals("_:a", a);
			assertEquals("_:b", b);
		}
		assertEquals("\"lit\"", p2.getObject().toString());
		assertEquals("http://ex/s1", p3.getSubject().toString());
	}

	private void assertMintsBlankNodesWithoutGraph(Map<String, QuadString> byPredicate) {
		QuadString p1 = byPredicate.get(P1);
		QuadString p2 = byPredicate.get(P2);
		QuadString p3 = byPredicate.get(P3);

		String a = p1.getSubject().toString();
		String b = p1.getObject().toString();

		assertTrue(a.startsWith("_:"));
		assertTrue(b.startsWith("_:"));
		assertEquals(a, p2.getSubject().toString());
		assertEquals(b, p3.getObject().toString());

		assertNotEquals("_:a", a);
		assertNotEquals("_:b", b);
		assertNotEquals(a, b);
		assertEquals("\"lit\"", p2.getObject().toString());
		assertEquals("http://ex/s1", p3.getSubject().toString());
	}

	private void assertKeepsBlankNodesWithGraph(Map<String, QuadString> byPredicate, boolean expectOriginalLabels) {
		QuadString p1 = byPredicate.get(P1);
		QuadString p2 = byPredicate.get(P2);
		QuadString p3 = byPredicate.get(P3);
		QuadString p4 = byPredicate.get(P4);
		QuadString p5 = byPredicate.get(P5);

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

		if (expectOriginalLabels) {
			assertEquals("_:a", a);
			assertEquals("_:b", b);
			assertEquals("_:c", c);
			assertEquals("_:g", g);
		}

		assertEquals("http://ex/g1", p1.getGraph().toString());
		assertEquals("http://ex/g1", p2.getGraph().toString());
		assertEquals("http://ex/g1", p3.getGraph().toString());
		assertEquals("http://ex/o1", p4.getObject().toString());
	}

	private void assertMintsBlankNodesWithGraph(Map<String, QuadString> byPredicate) {
		QuadString p1 = byPredicate.get(P1);
		QuadString p2 = byPredicate.get(P2);
		QuadString p3 = byPredicate.get(P3);
		QuadString p4 = byPredicate.get(P4);
		QuadString p5 = byPredicate.get(P5);

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

		assertEquals("http://ex/g1", p1.getGraph().toString());
		assertEquals("http://ex/g1", p2.getGraph().toString());
		assertEquals("http://ex/g1", p3.getGraph().toString());
		assertEquals("http://ex/o1", p4.getObject().toString());
	}
}

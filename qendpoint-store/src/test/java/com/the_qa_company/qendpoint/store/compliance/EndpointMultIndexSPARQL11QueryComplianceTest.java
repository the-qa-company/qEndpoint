package com.the_qa_company.qendpoint.store.compliance;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIndexFile;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.EndpointStoreTest;
import com.the_qa_company.qendpoint.store.Utility;
import com.the_qa_company.qendpoint.utils.FileUtils;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11QueryComplianceTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.stream.Stream;

public class EndpointMultIndexSPARQL11QueryComplianceTest extends CTSPARQL11QueryComplianceTest {
	@TempDir
	public Path tempDirPath;

	public EndpointMultIndexSPARQL11QueryComplianceTest() {
		super(
				"constructwhere02 - CONSTRUCT WHERE",
				"constructwhere03 - CONSTRUCT WHERE",
				"constructwhere04 - CONSTRUCT WHERE",
				"Exists within graph pattern",
				"(pp07) Path with one graph",
				"(pp35) Named Graph 2",
				"sq01 - Subquery within graph pattern",
				"sq02 - Subquery within graph pattern, graph variable is bound",
				"sq03 - Subquery within graph pattern, graph variable is not bound",
				"sq04 - Subquery within graph pattern, default graph does not apply",
				"sq05 - Subquery within graph pattern, from named applies",
				"sq06 - Subquery with graph pattern, from named applies",
				"sq07 - Subquery with from ",
				"sq11 - Subquery limit per resource",
				"sq13 - Subqueries don't inject bindings",
				"sq14 - limit by resource");
	}

	@Override
	protected void testParameterListener(String displayName, String testURI, String name, String queryFileURL,
	                                     String resultFileURL, Dataset dataset, boolean ordered, boolean laxCardinality) {
		try {
			setUpHDT(dataset);
		} catch (IOException | ParserException | NotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected Sail newSail() throws Exception {
		FileUtils.deleteRecursively(tempDirPath);
		Path nativeStore = tempDirPath.resolve("ns");
		Path hdtStore = tempDirPath.resolve("hdt");

		Files.createDirectories(nativeStore);
		Files.createDirectories(hdtStore);

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS,
				EnumSet.of(TripleComponentOrder.SPO, TripleComponentOrder.OPS, TripleComponentOrder.PSO));
		Path fileName = Path.of(hdtStore.toAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME);
		if (this.hdt == null) {
			hdt = Utility.createTempHdtIndex(tempDirPath.resolve("test.nt"), true, false, spec);
		}
		assert hdt != null;

		hdt.saveToHDT(fileName, null);

		return  new EndpointStore(hdtStore.toAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME, spec,
				nativeStore.toAbsolutePath() + "/", true) {
			@Override
			public HDT loadIndex() throws IOException {
				HDT idx = super.loadIndex();
				if (idx.getTriples().getNumberOfElements() == 0) {
					return idx;
				}
				try {
					Path fileOPS = BitmapTriplesIndexFile.getIndexPath(fileName, TripleComponentOrder.OPS);
					Assertions.assertTrue(Files.exists(fileOPS), "can't find " + fileOPS);
					Path filePSO = BitmapTriplesIndexFile.getIndexPath(fileName, TripleComponentOrder.PSO);
					Assertions.assertTrue(Files.exists(filePSO), "can't find " + filePSO);
				} catch (Throwable t) {
					try (Stream<Path> l = Files.list(fileName.getParent())) {
						l.forEach(System.err::println);
					} catch (Exception e) {
						t.addSuppressed(e);
					}
					throw t;
				}
				return idx;
			}
		};
	}

	HDT hdt;

	private void setUpHDT(Dataset dataset) throws IOException, ParserException, NotFoundException {
		if (dataset == null) {
			return;
		}

		String x = dataset.getDefaultGraphs().toString();
		if (x.equals("[]")) {
			x = dataset.getNamedGraphs().toString();
		}
		String str = x.substring(x.lastIndexOf("!") + 1).replace("]", "");

		URL url = SPARQL11QueryComplianceTest.class.getResource(str);
		File tmpDir = new File("test");
		if (tmpDir.mkdirs()) {
			logger.debug("{} dir created.", tmpDir);
		}
		assert url != null;
		JarURLConnection con = (JarURLConnection) url.openConnection();
		File file = new File(tmpDir, con.getEntryName());

		HDTOptions spec = HDTOptions.of();

		hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.example.org/", RDFNotation.guess(file),
				spec, null);
		assert hdt != null;
		hdt.search("", "", "").forEachRemaining(System.out::println);
	}
}

package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIndexFile;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11QueryComplianceTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

public class EndpointMultIndexSPARQL11QueryComplianceTest extends SPARQL11QueryComplianceTest {
	private static final Logger logger = LoggerFactory.getLogger(EndpointSPARQL11QueryComplianceTest.class);

	public EndpointMultIndexSPARQL11QueryComplianceTest() {
		super();
		List<String> testToIgnore = new ArrayList<>();
		// @todo these tests are failing and should not, they are skipped so
		// that we can be sure that we see when
		// currently passing tests are not failing. Many of these tests are not
		// so problematic since we do not support
		// named graphs anyway
		testToIgnore.add("constructwhere02 - CONSTRUCT WHERE");
		testToIgnore.add("constructwhere03 - CONSTRUCT WHERE");
		testToIgnore.add("constructwhere04 - CONSTRUCT WHERE");
		testToIgnore.add("Exists within graph pattern");
		testToIgnore.add("(pp07) Path with one graph");
		testToIgnore.add("(pp35) Named Graph 2");
		testToIgnore.add("sq01 - Subquery within graph pattern");
		testToIgnore.add("sq02 - Subquery within graph pattern, graph variable is bound");
		testToIgnore.add("sq03 - Subquery within graph pattern, graph variable is not bound");
		testToIgnore.add("sq04 - Subquery within graph pattern, default graph does not apply");
		testToIgnore.add("sq05 - Subquery within graph pattern, from named applies");
		testToIgnore.add("sq06 - Subquery with graph pattern, from named applies");
		testToIgnore.add("sq07 - Subquery with from ");
		testToIgnore.add("sq11 - Subquery limit per resource");
		testToIgnore.add("sq13 - Subqueries don't inject bindings");
		testToIgnore.add("sq14 - limit by resource");

		this.setIgnoredTests(testToIgnore);
	}

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	EndpointStore endpoint;
	File nativeStore;
	File hdtStore;

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
	protected Repository newRepository() throws Exception {
		nativeStore = tempDir.newFolder();
		hdtStore = tempDir.newFolder();

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS,
				EnumSet.of(TripleComponentOrder.SPO, TripleComponentOrder.OPS, TripleComponentOrder.PSO));
		Path fileName = Path.of(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME);
		if (this.hdt == null) {
			hdt = Utility.createTempHdtIndex(tempDir, true, false, spec);
		}
		assert hdt != null;

		hdt.saveToHDT(fileName, null);

		endpoint = new EndpointStore(hdtStore.getAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + "/", true) {
			@Override
			public HDT loadIndex() throws IOException {
				HDT idx = super.loadIndex();
				if (idx.getTriples().getNumberOfElements() == 0) {
					return idx;
				}
				try {
					Path fileOPS = BitmapTriplesIndexFile.getIndexPath(fileName, TripleComponentOrder.OPS);
					Assert.assertTrue("can't find " + fileOPS, Files.exists(fileOPS));
					Path filePSO = BitmapTriplesIndexFile.getIndexPath(fileName, TripleComponentOrder.PSO);
					Assert.assertTrue("can't find " + filePSO, Files.exists(filePSO));
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
		// endpoint.setThreshold(2);
		return new SailRepository(endpoint);
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

		hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.example.org/", RDFNotation.guess(file), spec,
				null);
		assert hdt != null;
		hdt.search("", "", "").forEachRemaining(System.out::println);
	}
}

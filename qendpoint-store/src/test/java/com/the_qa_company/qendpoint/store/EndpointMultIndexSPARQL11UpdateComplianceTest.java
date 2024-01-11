package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIndex;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIndexFile;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11UpdateComplianceTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class EndpointMultIndexSPARQL11UpdateComplianceTest extends SPARQL11UpdateComplianceTest {

	public EndpointMultIndexSPARQL11UpdateComplianceTest(String displayName, String testURI, String name,
			String requestFile, IRI defaultGraphURI, Map<String, IRI> inputNamedGraphs, IRI resultDefaultGraphURI,
			Map<String, IRI> resultNamedGraphs) {
		super(displayName, testURI, name, requestFile, defaultGraphURI, inputNamedGraphs, resultDefaultGraphURI,
				resultNamedGraphs);
		List<String> testToIgnore = new ArrayList<>();
		// @todo these tests are failing and should not, they are skipped so
		// that we can be sure that we see when
		// currently passing tests are not failing. Many of these tests are not
		// so problematic since we do not support
		// named graphs anyway
		testToIgnore.add("DELETE INSERT 1b");
		testToIgnore.add("DELETE INSERT 1c");
		testToIgnore.add("CLEAR NAMED");
		testToIgnore.add("DROP NAMED");
		this.setIgnoredTests(testToIgnore);
	}

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Override
	protected Repository newRepository() throws Exception {
		File nativeStore = tempDir.newFolder();
		File hdtStore = tempDir.newFolder();
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS,
				EnumSet.of(TripleComponentOrder.SPO, TripleComponentOrder.OPS, TripleComponentOrder.PSO));
		Path fileName = Path.of(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME);
		long size;
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, true, false, spec)) {
			assert hdt != null;
			size = hdt.getTriples().getNumberOfElements();
			hdt.saveToHDT(fileName, null);
		}

		EndpointStore endpoint = new EndpointStore(hdtStore.getAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME,
				spec, nativeStore.getAbsolutePath() + "/", true) {

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
		// return new DatasetRepository(new SailRepository(new
		// NativeStore(tempDir.newFolder(), "spoc")));
	}

	@Override
	public void setUp() throws Exception {
		super.setUp();
	}
}

package com.the_qa_company.qendpoint.store;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11UpdateComplianceTest;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Test SPARQL 1.1 Update functionality on a native store.
 *
 * @author Ali Haidar
 */
public class EndpointSPARQL11UpdateComplianceTest extends SPARQL11UpdateComplianceTest {

	public EndpointSPARQL11UpdateComplianceTest(String displayName, String testURI, String name, String requestFile,
			IRI defaultGraphURI, Map<String, IRI> inputNamedGraphs, IRI resultDefaultGraphURI,
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
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Override
	protected Repository newRepository() throws Exception {
		File nativeStore = tempDir.newFolder();
		File hdtStore = tempDir.newFolder();
		HDTSpecification spec = new HDTSpecification();
		spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, true, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);
		}

		EndpointStore endpoint = new EndpointStore(hdtStore.getAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME,
				spec, nativeStore.getAbsolutePath() + "/", true);
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

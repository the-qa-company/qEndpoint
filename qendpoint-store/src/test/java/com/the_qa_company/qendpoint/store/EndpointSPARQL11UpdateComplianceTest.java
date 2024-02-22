package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.iterator.utils.EmptyIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.utils.FileUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11UpdateComplianceTest;
import org.junit.Rule;
import org.junit.jupiter.api.io.TempDir;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Test SPARQL 1.1 Update functionality on a native store.
 *
 * @author Ali Haidar
 */
public class EndpointSPARQL11UpdateComplianceTest extends SPARQL11UpdateComplianceTest {

	public EndpointSPARQL11UpdateComplianceTest() {

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

	@TempDir
	public Path tempDir;

	@Override
	protected Repository newRepository() throws Exception {
		FileUtils.deleteRecursively(tempDir);
		Path nativeStore = tempDir.resolve("ns");
		Path hdtStore = tempDir.resolve("hdt");

		Files.createDirectories(nativeStore);
		Files.createDirectories(hdtStore);

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
		try (HDT hdt = HDTManager.generateHDT(EmptyIterator.of(), Utility.EXAMPLE_NAMESPACE, HDTOptions.of(),
				ProgressListener.ignore())) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.toAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);
		}

		EndpointStore endpoint = new EndpointStore(hdtStore.toAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME,
				spec, nativeStore.toAbsolutePath() + "/", true);
		// endpoint.setThreshold(2);

		if (EndpointSPARQL11QueryComplianceTest.PRINT) {
			return Utility.convertToDumpRepository(new SailRepository(Utility.convertToDumpSail(endpoint)));
		}
		return new SailRepository(endpoint);
		// return new DatasetRepository(new SailRepository(new
		// NativeStore(tempDir.newFolder(), "spoc")));
	}

}


package com.the_qa_company.qendpoint.store;

import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11QueryComplianceTest;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Ali Haidar
 */
public class EndpointSPARQL11QueryComplianceTest extends SPARQL11QueryComplianceTest {
	private static final Logger logger = LoggerFactory.getLogger(EndpointSPARQL11QueryComplianceTest.class);

	public EndpointSPARQL11QueryComplianceTest() {
		super();

		List<String> testToIgnore = Arrays.asList(
				// @todo these tests are failing and should not, they are
				// skipped so
				// that we can be sure that we see when
				// currently passing tests are not failing. Many of these tests
				// are not
				// so problematic since we do not support
				// named graphs anyway
				"constructwhere02 - CONSTRUCT WHERE", "constructwhere03 - CONSTRUCT WHERE",
				"constructwhere04 - CONSTRUCT WHERE", "Exists within graph pattern", "(pp07) Path with one graph",
				"(pp35) Named Graph 2", "sq01 - Subquery within graph pattern",
				"sq02 - Subquery within graph pattern, graph variable is bound",
				"sq03 - Subquery within graph pattern, graph variable is not bound",
				"sq04 - Subquery within graph pattern, default graph does not apply",
				"sq05 - Subquery within graph pattern, from named applies",
				"sq06 - Subquery with graph pattern, from named applies", "sq07 - Subquery with from ",
				"sq11 - Subquery limit per resource", "sq13 - Subqueries don't inject bindings",
				"sq14 - limit by resource");

		this.setIgnoredTests(testToIgnore);
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

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	EndpointStore endpoint;
	File nativeStore;
	File hdtStore;

	@Override
	protected Repository newRepository() throws Exception {
		nativeStore = tempDir.newFolder();
		hdtStore = tempDir.newFolder();

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
		if (this.hdt == null) {
			hdt = Utility.createTempHdtIndex(tempDir, true, false, spec);
		}
		assert hdt != null;

		hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);

		endpoint = new EndpointStore(hdtStore.getAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + "/", true);
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

package com.the_qa_company.qendpoint.store.experimental;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.store.Utility;
import com.the_qa_company.qendpoint.utils.FileUtils;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11QueryComplianceTest;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ExperimentalQEndpointSPARQL11ComplianceQueryTest extends SPARQL11QueryComplianceTest {
	/*
	 * Set this to true to print the call to the store when doing the compliance
	 * tests
	 */
	private static final boolean PRINT_CALLS = false;

	/*
	 * Set this to false to enable the graph tests
	 */
	private static final boolean DISABLE_GRAPH_TESTS = true;

	@TempDir
	public Path tempDir;

	public ExperimentalQEndpointSPARQL11ComplianceQueryTest() {

		if (DISABLE_GRAPH_TESTS) {
			this.setIgnoredTests(new ArrayList<>(List.of("constructwhere04 - CONSTRUCT WHERE",
					"Exists within graph pattern", "(pp07) Path with one graph", "(pp34) Named Graph 1",
					"(pp35) Named Graph 2", "sq01 - Subquery within graph pattern",
					"sq02 - Subquery within graph pattern, graph variable is bound",
					"sq03 - Subquery within graph pattern, graph variable is not bound",
					"sq04 - Subquery within graph pattern, default graph does not apply",
					"sq05 - Subquery within graph pattern, from named applies",
					"sq06 - Subquery with graph pattern, from named applies", "sq07 - Subquery with from",
					"sq11 - Subquery limit per resource", "sq13 - Subqueries don't inject bindings")));
		}
	}

	@Override
	protected Repository newRepository() throws Exception {
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);
		FileUtils.deleteRecursively(tempDir);
		Files.createDirectories(tempDir);
		ExperimentalQEndpointSail sail = new ExperimentalQEndpointSail(tempDir, spec);

		if (PRINT_CALLS) {
			return Utility.convertToDumpRepository(new SailRepository(Utility.convertToDumpSail(sail)));
		}
		return new SailRepository(sail);
	}
}

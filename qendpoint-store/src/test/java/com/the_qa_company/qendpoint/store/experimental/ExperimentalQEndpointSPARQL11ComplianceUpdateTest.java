package com.the_qa_company.qendpoint.store.experimental;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.store.Utility;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11UpdateComplianceTest;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ExperimentalQEndpointSPARQL11ComplianceUpdateTest extends SPARQL11UpdateComplianceTest {
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

	public ExperimentalQEndpointSPARQL11ComplianceUpdateTest() {

		if (DISABLE_GRAPH_TESTS) {
			this.setIgnoredTests(new ArrayList<>(List.of("INSERT 03", "INSERT 04", "INSERT USING 01",
					"DELETE INSERT 1b", "DELETE INSERT 1c", "INSERT same bnode twice", "CLEAR NAMED", "DROP NAMED",
					"DROP GRAPH", "DROP DEFAULT", "CLEAR GRAPH", "CLEAR DEFAULT", "COPY 1", "COPY 3", "COPY 6",
					"MOVE 1", "MOVE 3", "MOVE 6", "Graph-specific DELETE DATA 1", "Graph-specific DELETE DATA 2",
					"Graph-specific DELETE 1", "Graph-specific DELETE 1 (WITH)", "Graph-specific DELETE 1 (USING)",
					"Simple DELETE 1 (USING)", "Simple DELETE 2 (WITH)", "Simple DELETE 4 (WITH)")));
		}
	}

	@Override
	protected Repository newRepository() throws Exception {
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);
		ExperimentalQEndpointSail sail = new ExperimentalQEndpointSail(tempDir, spec);

		if (PRINT_CALLS) {
			return Utility.convertToDumpRepository(new SailRepository(Utility.convertToDumpSail(sail)));
		}
		return new SailRepository(sail);
	}

}

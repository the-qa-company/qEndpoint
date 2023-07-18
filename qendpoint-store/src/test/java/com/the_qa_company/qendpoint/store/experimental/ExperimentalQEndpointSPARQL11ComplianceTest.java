package com.the_qa_company.qendpoint.store.experimental;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.store.Utility;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11QueryComplianceTest;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11UpdateComplianceTest;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(Suite.class)
@Suite.SuiteClasses({ ExperimentalQEndpointSPARQL11ComplianceTest.UpdateTest.class,
		ExperimentalQEndpointSPARQL11ComplianceTest.QueryTest.class })
public class ExperimentalQEndpointSPARQL11ComplianceTest {
	/*
	 * Set this to true to print the call to the store when doing the compliance
	 * tests
	 */
	private static final boolean PRINT_CALLS = false;
	/*
	 * Set this to false to enable the graph tests
	 */
	private static final boolean DISABLE_GRAPH_TESTS = true;

	private static Repository createRepo(TemporaryFolder tempDir) throws IOException {
		Path root = tempDir.newFolder().toPath();
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);
		ExperimentalQEndpointSail sail = new ExperimentalQEndpointSail(root, spec);
		if (PRINT_CALLS) {
			return Utility.convertToDumpRepository(new SailRepository(Utility.convertToDumpSail(sail)));
		}
		return new SailRepository(sail);
	}

	public static class QueryTest extends SPARQL11QueryComplianceTest {
		@Rule
		public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

		public QueryTest(String displayName, String testURI, String name, String queryFileURL, String resultFileURL,
				Dataset dataset, boolean ordered, boolean laxCardinality) {
			super(displayName, testURI, name, queryFileURL, resultFileURL, dataset, ordered, laxCardinality);

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
			return createRepo(tempDir);
		}
	}

	public static class UpdateTest extends SPARQL11UpdateComplianceTest {
		@Rule
		public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

		public UpdateTest(String displayName, String testURI, String name, String requestFile, IRI defaultGraphURI,
				Map<String, IRI> inputNamedGraphs, IRI resultDefaultGraphURI, Map<String, IRI> resultNamedGraphs) {
			super(displayName, testURI, name, requestFile, defaultGraphURI, inputNamedGraphs, resultDefaultGraphURI,
					resultNamedGraphs);

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
			return createRepo(tempDir);
		}
	}
}

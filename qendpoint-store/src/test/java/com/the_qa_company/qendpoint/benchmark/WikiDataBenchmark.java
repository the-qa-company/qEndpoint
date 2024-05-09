package com.the_qa_company.qendpoint.benchmark;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.EndpointStore;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.InnerMergeJoinIterator;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 10)
@BenchmarkMode({ Mode.AverageTime })
//@Fork(value = 1, jvmArgs = { "-Xms96G", "-Xmx96G", "-XX:+UnlockExperimentalVMOptions","-XX:+UseEpsilonGC", "-XX:+AlwaysPreTouch" })
//@Fork(value = 1, jvmArgs = { "-Xms32G", "-Xmx32G", "-XX:+AlwaysPreTouch" })
@Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G", "-XX:+AlwaysPreTouch" })
//@Fork(value = 3, jvmArgs = { "-Xms4G", "-Xmx4G", "-XX:+AlwaysPreTouch", "-XX:+PrintCompilation","-XX:+UnlockDiagnosticVMOptions","-XX:+PrintInlining" })
//@Fork(value = 3, jvmArgs = { "-Xms4G", "-Xmx4G", "-XX:+EnableDynamicAgentLoading", "-XX:+AlwaysPreTouch", "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints", "-XX:FlightRecorderOptions=stackdepth=2048" })
//@Fork(value = 1, jvmArgs = { "-Xms32G", "-Xmx32G", "-XX:+EnableDynamicAgentLoading", "-XX:+AlwaysPreTouch","-XX:StartFlightRecording=delay=15s,dumponexit=true,filename=recording.jfr,method-profiling=max","-XX:FlightRecorderOptions=stackdepth=2048", "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints" })
@Measurement(iterations = 120, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class WikiDataBenchmark {

	private SailRepository endpointStore;

	@Setup(Level.Trial)
	public void setUp() throws IOException {

		Path dir = Path.of(System.getProperty("user.dir") + "/wdbench-indexes/");
		System.out.println("Loading from: " + dir);

// store options
		HDTOptions options = HDTOptions.of(
				// disable the default index (to use the custom indexes)
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true,
				// set the custom indexes we want
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "sop,ops,osp,pso,pos");

		EndpointStore store = new EndpointStore(new EndpointFiles(dir, "wdbench.hdt"), options);
		store.init();

		endpointStore = new SailRepository(store);

	}

	@TearDown(Level.Trial)
	public void tearDown() {
		if (endpointStore != null) {
			endpointStore.shutDown();
		}
		endpointStore = null;
	}

    public static void main(String[] args) throws IOException {
        Path dir = Path.of("/Users/havardottestad/Documents/Programming/qEndpoint2/qendpoint-store/wdbench-indexes");
        System.out.println("Loading from: " + dir);

        WikiDataBenchmark wikiDataBypassRDF4JBenchmark = new WikiDataBenchmark();
        HDTOptions options = HDTOptions.of(
                // disable the default index (to use the custom indexes)
                HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true,
                // set the custom indexes we want
                HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "sop,ops,osp,pso,pos");

        wikiDataBypassRDF4JBenchmark.endpointStore = new SailRepository(new EndpointStore(new EndpointFiles(dir, "wdbench.hdt"), options));
        wikiDataBypassRDF4JBenchmark.endpointStore.init();


        wikiDataBypassRDF4JBenchmark.testCountSimpleJoin2();
        wikiDataBypassRDF4JBenchmark.tearDown();
    }

	@Benchmark
	public long testCountSimpleJoin() {
		try (SailRepositoryConnection connection = endpointStore.getConnection()) {

			String query = """
					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
					          PREFIX wd: <http://www.wikidata.org/entity/>
					          PREFIX wdt: <http://www.wikidata.org/prop/direct/>
					          SELECT (COUNT(?profession_id) AS ?count) WHERE {
					            ?person_id wdt:P31 wd:Q5 .
					            ?person_id wdt:P106 ?profession_id .
					          }
					""";

			try (TupleQueryResult evaluate = connection.prepareTupleQuery(query).evaluate()) {
				long i = 0;
				while (evaluate.hasNext()) {
					i++;
					BindingSet next = evaluate.next();
					System.out.println(next);
					if (!next.toString().equals("[count=\"8501245\"^^<http://www.w3.org/2001/XMLSchema#integer>]")) {
						throw new IllegalStateException("Unexpected result: " + next);
					}
				}
				return i;
			}

		}
	}

	@Benchmark
	public long testCountSimpleJoin2() {

		try (SailRepositoryConnection connection = endpointStore.getConnection()) {

			String query = """
					PREFIX wd: <http://www.wikidata.org/entity/>
					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
					SELECT (count(?s) as ?c) WHERE {
					?s wdt:P106 ?o .
					  ?s wdt:P31 wd:Q5 .
					  ?s wdt:P21 ?sex
					}
					""";

			try (TupleQueryResult evaluate = connection.prepareTupleQuery(query).evaluate()) {
				long i = 0;
				while (evaluate.hasNext()) {
					i++;
					BindingSet next = evaluate.next();
					System.out.println(next);
					if (!next.toString().equals("[c=\"7011884\"^^<http://www.w3.org/2001/XMLSchema#integer>]")) {
						throw new IllegalStateException("Unexpected result: " + next);
					}
				}
				return i;
			}

		}
	}

	@Benchmark
	public long testCount() {
		try (SailRepositoryConnection connection = endpointStore.getConnection()) {

			String query = """
					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
					          PREFIX wd: <http://www.wikidata.org/entity/>
					          PREFIX wdt: <http://www.wikidata.org/prop/direct/>
					          SELECT (COUNT(?profession_id) AS ?count) WHERE {
					            ?person_id wdt:P106 ?profession_id .
					          }
					         """;

			try (TupleQueryResult evaluate = connection.prepareTupleQuery(query).evaluate()) {
				long i = 0;
				while (evaluate.hasNext()) {
					i++;
					BindingSet next = evaluate.next();
					System.out.println(next);
				}
				return i;
			}

		}
	}

	@Benchmark
	public long testCountWithoutCountInQuery() {
		try (SailRepositoryConnection connection = endpointStore.getConnection()) {

			String query = """
					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
					          PREFIX wd: <http://www.wikidata.org/entity/>
					          PREFIX wdt: <http://www.wikidata.org/prop/direct/>
					          SELECT ?profession_id WHERE {
					            ?person_id wdt:P106 ?profession_id .
					          }
					         """;

			try (TupleQueryResult evaluate = connection.prepareTupleQuery(query).evaluate()) {
				long i = 0;
				while (evaluate.hasNext()) {
					i++;
					evaluate.next();
				}
				return i;
			}

		}
	}

	@Benchmark
	public long testCountGetStatements() {
		try (SailRepositoryConnection connection = endpointStore.getConnection()) {

			try (RepositoryResult<Statement> statements = connection.getStatements(null,
					Values.iri("http://www.wikidata.org/prop/direct/P106"), null, false)) {
				long i = 0;
				while (statements.hasNext()) {
					i++;
					statements.next();
				}
				return i;
			}

		}
	}

}

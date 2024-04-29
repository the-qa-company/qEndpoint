package com.the_qa_company.qendpoint.benchmark;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.EndpointStoreTripleIterator;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 3)
@BenchmarkMode({ Mode.AverageTime })
@Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G", "-XX:+EnableDynamicAgentLoading" })
//@Fork(value = 1, jvmArgs = { "-Xms32G", "-Xmx32G", "-XX:+EnableDynamicAgentLoading" })
//@Fork(value = 1, jvmArgs = { "-Xms96G", "-Xmx96G", "-XX:+UnlockExperimentalVMOptions","-XX:+UseEpsilonGC", "-XX:+AlwaysPreTouch" })
//@Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G", "-XX:StartFlightRecording=delay=15s,duration=120s,filename=recording.jfr,settings=profile", "-XX:FlightRecorderOptions=samplethreads=true,stackdepth=2048", "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints"})
@Measurement(iterations = 3)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class WikiDataDifferentIndexesBenchmark {

	private SailRepository endpointStore;

	@State(Scope.Benchmark)
	public static class MyState {
		@Param({ "POS", "OSP", "PSO", "SOP", "OPS", "Unknown" })
		public TripleComponentOrder order;
	}

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
		BitmapTriples.useDefaultOrder = true;
		TripleComponentOrder.preference = null;
		EndpointStoreTripleIterator.cache = true;
	}

	@Benchmark
	public long testCount(MyState state, Blackhole blackhole) {
		EndpointStoreTripleIterator.cache = false;

		if (state.order == TripleComponentOrder.Unknown) {
			BitmapTriples.useDefaultOrder = true;
			TripleComponentOrder.preference = null;
		} else {
			BitmapTriples.useDefaultOrder = false;
			TripleComponentOrder.preference = state.order;
		}

		try (SailRepositoryConnection connection = endpointStore.getConnection()) {

			String query = """
					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
					          PREFIX wd: <http://www.wikidata.org/entity/>
					          PREFIX wdt: <http://www.wikidata.org/prop/direct/>
					          SELECT (COUNT(*) AS ?count) WHERE {
					          {
					          	SELECT * WHERE{
					            	?person_id ?p ?profession_id .
					          	} limit 1000000
					          }
					          }
					""";

			try (TupleQueryResult evaluate = connection.prepareTupleQuery(query).evaluate()) {
				long i = 0;
				while (evaluate.hasNext()) {
					blackhole.consume(evaluate.next());
					i++;
				}
				return i;
			}

		}
	}

}

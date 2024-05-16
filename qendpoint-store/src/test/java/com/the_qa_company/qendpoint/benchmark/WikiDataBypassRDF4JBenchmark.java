package com.the_qa_company.qendpoint.benchmark;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.model.SimpleIRIHDT;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.EndpointStoreTripleIterator;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.store.HDTConverter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.GenericStatement;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

//failed to inline
@State(Scope.Benchmark)
@Warmup(iterations = 0)
@BenchmarkMode({ Mode.AverageTime })
//@Fork(value = 1, jvmArgs = { "-Xms32G", "-Xmx32G", "-XX:+EnableDynamicAgentLoading", "-XX:+AlwaysPreTouch", "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints", "-XX:FlightRecorderOptions=stackdepth=2048" })
//@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G", "-XX:+EnableDynamicAgentLoading"})
//@Fork(value = 1, jvmArgs = { "-Xms32G", "-Xmx32G", "-XX:+EnableDynamicAgentLoading" , "-XX:-Inline"})
//@Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G", "-XX:+AlwaysPreTouch", "-XX:+PrintCompilation","-XX:+UnlockDiagnosticVMOptions","-XX:+PrintInlining" })
//@Fork(value = 1, jvmArgs = { "-Xms96G", "-Xmx96G", "-XX:+UnlockExperimentalVMOptions","-XX:+UseEpsilonGC", "-XX:+AlwaysPreTouch" })
//@Fork(value = 1, jvmArgs = { "-Xms32G", "-Xmx32G", "-XX:StartFlightRecording:delay=15s,duration=600s,filename=recording.jfr,settings=profile,method-profiling=max", "-XX:FlightRecorderOptions:stackdepth=2048,globalbuffersize=1024M", "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints"})
@Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G",
		"-XX:StartFlightRecording:dumponexit=true,filename=recording.jfr,settings=profile,method-profiling=max",
		"-XX:FlightRecorderOptions:stackdepth=2048,globalbuffersize=1024M", "-XX:+UnlockDiagnosticVMOptions",
		"-XX:+DebugNonSafepoints" })
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class WikiDataBypassRDF4JBenchmark {

	private EndpointStore endpointStore;

	private ArrayDeque<TripleID> objects;

	private static long counter = 0;

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

		endpointStore = new EndpointStore(new EndpointFiles(dir, "wdbench.hdt"), options);
		endpointStore.init();

		{

			HDT hdt = endpointStore.getHdt();

			final String wdt = "http://www.wikidata.org/prop/direct/";
			final String wd = "http://www.wikidata.org/entity/";

			Dictionary d = hdt.getDictionary();
			long p31 = d.stringToId(wdt + "P31", TripleComponentRole.PREDICATE);
			long p131 = d.stringToId(wdt + "P131", TripleComponentRole.PREDICATE);
			// human
			long q5 = d.stringToId(wd + "Q5", TripleComponentRole.OBJECT);
			// museum
			long q33506 = d.stringToId(wd + "Q33506", TripleComponentRole.OBJECT);
			// roma
			long q220 = d.stringToId(wd + "Q220", TripleComponentRole.OBJECT);
			// roma
			long p106 = d.stringToId(wdt + "P106", TripleComponentRole.PREDICATE);

			IteratorTripleID it = hdt.getTriples().search(new TripleID(0, p106, 0));

			TripleComponentOrder order = it.getOrder();
//        System.out.println(order);

			long count = 0;

			HDTConverter hdtConverter = new HDTConverter(endpointStore);
			objects = new ArrayDeque<>(1024);
			while (it.hasNext()) {

				objects.addLast(it.next());
			}

		}

	}

	@Setup(Level.Invocation)
	public void setupInvocation() {
		HDT hdt = endpointStore.getHdt();
		IteratorTripleID it = hdt.getTriples().search(new TripleID(0, 0, 0));
		while (it.hasNext()) {
			it.next();
		}
	}

	@TearDown(Level.Trial)
	public void tearDown() {
		if (endpointStore != null) {
			endpointStore.shutDown();
		}
		endpointStore = null;
		System.out.println(counter);
	}

//    public static void main(String[] args) throws IOException {
//        Path dir = Path.of("/Users/havardottestad/Documents/Programming/qEndpoint2/qendpoint-store/wdbench-indexes");
//        System.out.println("Loading from: " + dir);
//
//        WikiDataBypassRDF4JBenchmark wikiDataBypassRDF4JBenchmark = new WikiDataBypassRDF4JBenchmark();
//        HDTOptions options = HDTOptions.of(
//                // disable the default index (to use the custom indexes)
//                HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true,
//                // set the custom indexes we want
//                HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "sop,ops,osp,pso,pos");
//
//        wikiDataBypassRDF4JBenchmark.endpointStore = new EndpointStore(new EndpointFiles(dir, "wdbench.hdt"), options);
//        wikiDataBypassRDF4JBenchmark.endpointStore.init();
//
//
//        wikiDataBypassRDF4JBenchmark.testCountGetStatements();
//        wikiDataBypassRDF4JBenchmark.tearDown();
//    }

	@Benchmark
	public long testCountDirect(Blackhole blackhole) throws IOException {

		HDT hdt = endpointStore.getHdt();

		final String wdt = "http://www.wikidata.org/prop/direct/";
		final String wd = "http://www.wikidata.org/entity/";

		Dictionary d = hdt.getDictionary();
		long p31 = d.stringToId(wdt + "P31", TripleComponentRole.PREDICATE);
		long p131 = d.stringToId(wdt + "P131", TripleComponentRole.PREDICATE);
		// human
		long q5 = d.stringToId(wd + "Q5", TripleComponentRole.OBJECT);
		// museum
		long q33506 = d.stringToId(wd + "Q33506", TripleComponentRole.OBJECT);
		// roma
		long q220 = d.stringToId(wd + "Q220", TripleComponentRole.OBJECT);
		// roma
		long p106 = d.stringToId(wdt + "P106", TripleComponentRole.PREDICATE);

		IteratorTripleID it = hdt.getTriples().search(new TripleID(0, p106, 0));

		TripleComponentOrder order = it.getOrder();
//        System.out.println(order);

		long count = 0;

		HDTConverter hdtConverter = new HDTConverter(endpointStore);

		while (it.hasNext()) {
			TripleID tid = it.next();
			blackhole.consume(tid);

			Resource resource = hdtConverter.idToSubjectHDTResource(tid.getSubject());
			IRI iri = hdtConverter.idToPredicateHDTResource(tid.getPredicate());
			Value value = hdtConverter.idToObjectHDTResource(tid.getObject());
			GenericStatement<Resource, IRI, Value> resourceIRIValueGenericStatement = new GenericStatement<>(resource,
					iri, value, null);
			blackhole.consume(resourceIRIValueGenericStatement);

//            ArrayDeque<TripleID> objects = new ArrayDeque<>(1024);
//
//            for (int i = 0; i < 1024 && it.hasNext(); i++) {
//                objects.addLast(it.next());
//            }
//
//
//            for (TripleID tid : objects) {
//                Resource resource = hdtConverter.idToSubjectHDTResource(tid.getSubject());
//                IRI iri = hdtConverter.idToPredicateHDTResource(tid.getPredicate());
//                Value value = hdtConverter.idToObjectHDTResource(tid.getObject());
//                GenericStatement<Resource, IRI, Value> resourceIRIValueGenericStatement = new GenericStatement<>(resource, iri, value, null);
//                blackhole.consume(resourceIRIValueGenericStatement);
//            }

			count++;
		}

//        System.out.println(count);

		return count;

	}

	@Benchmark
	public long testOverhead(Blackhole blackhole) throws IOException {

		HDTConverter hdtConverter = new HDTConverter(endpointStore);

		long count = 1;

		for (TripleID tid : objects) {
			Resource resource = hdtConverter.idToSubjectHDTResource(tid.getSubject());
			IRI iri = hdtConverter.idToPredicateHDTResource(tid.getPredicate());
			Value value = hdtConverter.idToObjectHDTResource(tid.getObject());

			if (resource instanceof SimpleIRIHDT) {
				counter++;
			}

			if (iri instanceof SimpleIRIHDT) {
				counter++;
			}

			if (value instanceof SimpleIRIHDT) {
				counter++;
			}

			blackhole.consume(resource);
			blackhole.consume(iri);
			blackhole.consume(value);
//            MyGenericStatement obj = new MyGenericStatement(resource, iri, value, null);
//            blackhole.consume(obj);
		}

		return count;

	}

	public record MyGenericStatement(Resource getSubject, IRI getPredicate, Value getObject, Resource getContext)
			implements Statement {

		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else if (!(o instanceof Statement that)) {
				return false;
			} else {
				return this.getSubject.equals(that.getSubject()) && getPredicate.equals(that.getPredicate())
						&& getObject.equals(that.getObject()) && Objects.equals(getContext, that.getContext());
			}
		}

		public int hashCode() {
			int result = 1;
			result = 31 * result + getSubject.hashCode();
			result = 31 * result + getPredicate.hashCode();
			result = 31 * result + getObject.hashCode();
			result = 31 * result + (getContext == null ? 0 : getContext.hashCode());
			return result;
		}

		public String toString() {
			return "(" + getSubject + ", " + getPredicate + ", " + getObject + ") [" + getContext + "]";
		}
	}

	@Benchmark
	public long testOriginalOverhead_original(Blackhole blackhole) throws IOException {

		HDTConverter hdtConverter = new HDTConverter(endpointStore);

		long count = 1;

		for (TripleID tid : objects) {
			Resource resource = hdtConverter.idToSubjectHDTResource(tid.getSubject());
			IRI iri = hdtConverter.idToPredicateHDTResource(tid.getPredicate());
			Value value = hdtConverter.idToObjectHDTResource(tid.getObject());

			blackhole.consume(resource);
			blackhole.consume(iri);
			blackhole.consume(value);
			blackhole.consume(new GenericStatement<>(resource, iri, value, null));
		}

		return count;

	}

	@Benchmark
	public long testCountSPARQL() {
		EndpointStoreTripleIterator.cache = false;
		SailRepository sailRepository = new SailRepository(endpointStore);
		try (SailRepositoryConnection connection = sailRepository.getConnection()) {

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
	public long testCountGetStatements(Blackhole blackhole) {
		EndpointStoreTripleIterator.cache = false;
		SailRepository sailRepository = new SailRepository(endpointStore);

		try (SailRepositoryConnection connection = sailRepository.getConnection()) {

			try (RepositoryResult<Statement> statements = connection.getStatements(null,
					Values.iri("http://www.wikidata.org/prop/direct/P106"), null, false)) {
				long i = 0;
				while (statements.hasNext()) {
					i++;
					Statement next = statements.next();
					blackhole.consume(next);
				}
				System.out.println(i);
				return i;
			}

		}
	}

}

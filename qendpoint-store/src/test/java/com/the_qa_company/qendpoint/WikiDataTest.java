//@formatter:off

//package com.the_qa_company.qendpoint;
//
//import com.the_qa_company.qendpoint.core.options.HDTOptions;
//import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
//import com.the_qa_company.qendpoint.model.HDTValue;
//import com.the_qa_company.qendpoint.store.EndpointFiles;
//import com.the_qa_company.qendpoint.store.EndpointStore;
//import com.the_qa_company.qendpoint.store.EndpointTripleSource;
//import org.apache.commons.lang3.time.StopWatch;
//import org.eclipse.rdf4j.query.BindingSet;
//import org.eclipse.rdf4j.query.QueryResults;
//import org.eclipse.rdf4j.query.TupleQuery;
//import org.eclipse.rdf4j.query.TupleQueryResult;
//import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.QueryJoinOptimizer;
//import org.eclipse.rdf4j.query.explanation.Explanation;
//import org.eclipse.rdf4j.repository.sail.SailRepository;
//import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
//import org.junit.jupiter.api.*;
//
//import java.io.IOException;
//import java.nio.file.Path;
//import java.util.Comparator;
//import java.util.Set;
//
//public class WikiDataTest {
//
//	private static SailRepository endpointStore;
//
//	@BeforeAll
//	public static void beforeAll() throws IOException {
//		Path dir = Path.of(System.getProperty("user.dir") + "/wdbench-indexes/");
//		System.out.println("Loading from: " + dir);
//
//// store options
//		HDTOptions options = HDTOptions.of(
//				// disable the default index (to use the custom indexes)
//				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true,
//				// set the custom indexes we want
//				HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "sop,ops,osp,pso,pos");
//
//		EndpointStore store = new EndpointStore(new EndpointFiles(dir, "wdbench.hdt"), options);
//		store.init();
//
//		endpointStore = new SailRepository(store);
//	}
//
//	@AfterAll
//	public static void afterAll() {
//		if (endpointStore != null) {
//			endpointStore.shutDown();
//		}
//		endpointStore = null;
//	}
//
//	@AfterEach
//	public void afterEach() {
//		EndpointTripleSource.ENABLE_MERGE_JOIN = true;
//		QueryJoinOptimizer.MERGE_JOIN_CARDINALITY_SIZE_DIFF_MULTIPLIER = 10;
//	}
//
//	@Test
//	public void testMergeJoinOnSubject() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       SELECT (count(?x1) as ?count) WHERE {
//					         ?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q13442814> .
//					        ?x1 wdt:P1476 ?c.
//					         }
//					         """;
//
//			String explanation = runQuery(connection, query);
//			Assertions.assertTrue(explanation.contains("JoinIterator) (resultSizeActual=37.4M, "));
//			Assertions.assertTrue(explanation
//					.contains("   │  ╠══ Join (InnerMergeJoinIterator) (resultSizeActual=37.4M, totalTimeActual="));
//
//		}
//
//	}
//
//	@Test
//	public void testScholarlyArticlesWithTitles() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       SELECT (count(?x1) as ?count) WHERE {
//					         ?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q13442814> .
//					        ?x1 wdt:P1476 ?c.
//					         }
//					         """;
//
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
///*
//Projection (resultSizeActual=1, totalTimeActual=12.5s, selfTimeActual=0.015ms)
//╠══ ProjectionElemList
//║     ProjectionElem "count"
//╚══ Extension (resultSizeActual=1, totalTimeActual=12.5s, selfTimeActual=0.003ms)
//   ├── Group () (resultSizeActual=1, totalTimeActual=12.5s, selfTimeActual=2.0s)
//   │  ╠══ Join (InnerMergeJoinIterator) (resultSizeActual=37.4M, totalTimeActual=10.5s, selfTimeActual=3.8s)
//   │  ║  ├── StatementPattern [statementOrder: S]  (costEstimate=37.3M, resultSizeEstimate=74.6M, resultSizeActual=37.2M, totalTimeActual=3.2s, selfTimeActual=3.2s) [left]
//   │  ║  │     s: Var (name=x1)
//   │  ║  │     p: Var (name=_const_e5f28efe_uri, value=http://www.wikidata.org/prop/direct/P31, anonymous)
//   │  ║  │     o: Var (name=_const_5b5b226_uri, value=http://www.wikidata.org/entity/Q13442814, anonymous)
//   │  ║  └── StatementPattern [statementOrder: S]  (costEstimate=9.0K, resultSizeEstimate=81.8M, resultSizeActual=40.9M, totalTimeActual=3.5s, selfTimeActual=3.5s) [right]
//   │  ║        s: Var (name=x1)
//   │  ║        p: Var (name=_const_3389f0e2_uri, value=http://www.wikidata.org/prop/direct/P1476, anonymous)
//   │  ║        o: Var (name=c)
//   │  ╚══ GroupElem (count)
//   │        Count
//   │           Var (name=x1)
//   └── ExtensionElem (count)
//         Count
//            Var (name=x1)
// */
//
//
//			EndpointTripleSource.ENABLE_MERGE_JOIN = false;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
///*
//
// */
//
//
//		}
//
//	}
//
//	@Test
//	public void testMergeJoinOnObject() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       SELECT * WHERE {
//								?country1 wdt:P38 ?currency.               # Country has currency
//								?country2 wdt:P38 ?currency.               # Country has currency
//								filter(?country1 != ?country2)
//							}
//					         """;
//
//			String explanation = runQuery(connection, query);
//			Assertions.assertTrue(explanation.contains("Projection (resultSizeActual=30.1K, "));
//			Assertions.assertTrue(explanation.contains("╚══ Filter (resultSizeActual=30.1K, "));
//			Assertions.assertTrue(explanation.contains("Iterator) (resultSizeActual=31.9K, "));
//			Assertions
//					.assertTrue(explanation.contains("   └── Join (InnerMergeJoinIterator) (resultSizeActual=31.9K, "));
//
//		}
//
//	}
//
//	@Test
//	public void testMergeJoinOnObjectWithFilter() {
//
////		EndpointTripleSource.ENABLE_MERGE_JOIN = false;
//
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       SELECT * WHERE {
//								?country1 wdt:P38 ?currency.               # Country has currency
//								?country2 wdt:P38 ?currency.               # Country has currency
//								FILTER(?currency IN (<http://www.wikidata.org/entity/Q1248202>,<http://www.wikidata.org/entity/Q125999>, <http://www.wikidata.org/entity/Q4916>))
//								FILTER(?country1 != ?country2)
//
//							}
//					         """;
//
//			String explanation = runQuery(connection, query);
//			Assertions.assertTrue(explanation.contains("JoinIterator) (resultSizeActual=8.6K, "));
//			Assertions.assertTrue(explanation.contains("   └── Join (InnerMergeJoinIterator) (resultSizeActual=8.6K, "));
//		}
//
//	}
//
//	@Test
//	public void testRegularJoinUsedWhenMoreOptimal() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       SELECT (count(?x1) as ?count) WHERE {
//					         ?x1  <http://www.wikidata.org/prop/direct/P304>  "1795-1796" .
//					        ?x1 wdt:P1476 ?c.
//					         }
//					         """;
//
//			String explanation = runQuery(connection, query);
//			Assertions.assertTrue(explanation.contains("JoinIterator) (resultSizeActual=212, "));
//			Assertions.assertTrue(explanation.contains("│  ╠══ Join (JoinIterator) (resultSizeActual=212, "));
//
//		}
//
//	}
//
//
//@Test
//	public void temp() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       select * where {
//					      ?x1 <http://www.wikidata.org/prop/direct/P106> <http://www.wikidata.org/entity/Q593644> . ?x1 <http://www.wikidata.org/prop/direct/P21> <http://www.wikidata.org/entity/Q6581072> . ?x2 <http://www.wikidata.org/prop/direct/P625> ?x3 . OPTIONAL { ?x1 <http://www.wikidata.org/prop/direct/P18> ?x4 . ?x1 <http://www.wikidata.org/prop/direct/P19> ?x2 . ?x1 <http://www.wikidata.org/prop/direct/P569> ?x5 . }
//					       } limit 100000
//					         """;
//			String explanation = runQuery(connection, query);
//			System.out.println(explanation);
//
//		}
//
//	}
//
//
//	@Test
//	public void testNumberOfPeopleThatAreTennisPlayers() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       SELECT (count(?x1) as ?count) WHERE {
//					         	?x1  <http://www.wikidata.org/prop/direct/P31>  wd:Q5 .
//								?x1  <http://www.wikidata.org/prop/direct/P106>  wd:Q10833314 .
//					         }
//
//
//					         """;
//
//			QueryJoinOptimizer.MERGE_JOIN_CARDINALITY_SIZE_DIFF_MULTIPLIER = 10;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
///*
//Projection (resultSizeActual=1, totalTimeActual=931ms, selfTimeActual=0.002ms)
//╠══ ProjectionElemList
//║     ProjectionElem "count"
//╚══ Extension (resultSizeActual=1, totalTimeActual=931ms, selfTimeActual=0.002ms)
//   ├── Group () (resultSizeActual=1, totalTimeActual=931ms, selfTimeActual=1.91ms)
//   │  ╠══ Join (InnerMergeJoinIterator) (resultSizeActual=10.9K, totalTimeActual=929ms, selfTimeActual=216ms)
//   │  ║  ├── StatementPattern [statementOrder: S]  (costEstimate=11.0K, resultSizeEstimate=22.0K, resultSizeActual=11.0K, totalTimeActual=0.989ms, selfTimeActual=0.989ms) [left]
//   │  ║  │     s: Var (name=x1)
//   │  ║  │     p: Var (name=_const_d85f4957_uri, value=http://www.wikidata.org/prop/direct/P106, anonymous)
//   │  ║  │     o: Var (name=_const_6dc8acf8_uri, value=http://www.wikidata.org/entity/Q10833314, anonymous)
//   │  ║  └── StatementPattern [statementOrder: S]  (costEstimate=1, resultSizeEstimate=18.4M, resultSizeActual=9.1M, totalTimeActual=712ms, selfTimeActual=712ms) [right]
//   │  ║        s: Var (name=x1)
//   │  ║        p: Var (name=_const_e5f28efe_uri, value=http://www.wikidata.org/prop/direct/P31, anonymous)
//   │  ║        o: Var (name=_const_f822a47a_uri, value=http://www.wikidata.org/entity/Q5, anonymous)
//   │  ╚══ GroupElem (count)
//   │        Count
//   │           Var (name=x1)
//   └── ExtensionElem (count)
//         Count
//            Var (name=x1)
//*/
//
//			EndpointTripleSource.ENABLE_MERGE_JOIN = false;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
//
///*
//Projection (resultSizeActual=1, totalTimeActual=23.2ms, selfTimeActual=0.002ms)
//╠══ ProjectionElemList
//║     ProjectionElem "count"
//╚══ Extension (resultSizeActual=1, totalTimeActual=23.2ms, selfTimeActual=0.002ms)
//   ├── Group () (resultSizeActual=1, totalTimeActual=23.2ms, selfTimeActual=1.03ms)
//   │  ╠══ Join (JoinIterator) (resultSizeActual=10.9K, totalTimeActual=22.1ms, selfTimeActual=21.5ms)
//   │  ║  ├── StatementPattern (costEstimate=11.0K, resultSizeEstimate=22.0K, resultSizeActual=11.0K, totalTimeActual=0.661ms, selfTimeActual=0.661ms) [left]
//   │  ║  │     s: Var (name=x1)
//   │  ║  │     p: Var (name=_const_d85f4957_uri, value=http://www.wikidata.org/prop/direct/P106, anonymous)
//   │  ║  │     o: Var (name=_const_6dc8acf8_uri, value=http://www.wikidata.org/entity/Q10833314, anonymous)
//   │  ║  └── StatementPattern (costEstimate=1, resultSizeEstimate=18.4M, resultSizeActual=10.9K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [right]
//   │  ║        s: Var (name=x1)
//   │  ║        p: Var (name=_const_e5f28efe_uri, value=http://www.wikidata.org/prop/direct/P31, anonymous)
//   │  ║        o: Var (name=_const_f822a47a_uri, value=http://www.wikidata.org/entity/Q5, anonymous)
//   │  ╚══ GroupElem (count)
//   │        Count
//   │           Var (name=x1)
//   └── ExtensionElem (count)
//         Count
//            Var (name=x1)
//
//*/
//
//		}
//
//	}
//
//
//	@Test
//	public void testAllMuseumsInRome() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       SELECT * WHERE {
//					         	?x1  <http://www.wikidata.org/prop/direct/P31>  wd:Q33506 .
//								?x1  <http://www.wikidata.org/prop/direct/P159>  wd:Q220 .
//					         }
//					         """;
//
//			QueryJoinOptimizer.MERGE_JOIN_CARDINALITY_SIZE_DIFF_MULTIPLIER = 100;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
///*
//Projection (resultSizeActual=3, totalTimeActual=4.27ms, selfTimeActual=0.001ms)
//╠══ ProjectionElemList
//║     ProjectionElem "x1"
//╚══ Join (InnerMergeJoinIterator) (resultSizeActual=3, totalTimeActual=4.27ms, selfTimeActual=1.14ms)
//   ├── StatementPattern [statementOrder: S]  (costEstimate=1.3K, resultSizeEstimate=2.6K, resultSizeActual=1.2K, totalTimeActual=0.105ms, selfTimeActual=0.105ms) [left]
//   │     s: Var (name=x1)
//   │     p: Var (name=_const_d85f49f5_uri, value=http://www.wikidata.org/prop/direct/P159, anonymous)
//   │     o: Var (name=_const_7a0b68f5_uri, value=http://www.wikidata.org/entity/Q220, anonymous)
//   └── StatementPattern [statementOrder: S]  (costEstimate=1, resultSizeEstimate=81.1K, resultSizeActual=40.5K, totalTimeActual=3.02ms, selfTimeActual=3.02ms) [right]
//         s: Var (name=x1)
//         p: Var (name=_const_e5f28efe_uri, value=http://www.wikidata.org/prop/direct/P31, anonymous)
//         o: Var (name=_const_24e3a460_uri, value=http://www.wikidata.org/entity/Q33506, anonymous)
//
// */
//
//			EndpointTripleSource.ENABLE_MERGE_JOIN = false;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
///*
//Projection (resultSizeActual=3, totalTimeActual=2.5ms, selfTimeActual=0.002ms)
//╠══ ProjectionElemList
//║     ProjectionElem "x1"
//╚══ Join (JoinIterator) (resultSizeActual=3, totalTimeActual=2.49ms, selfTimeActual=2.39ms)
//   ├── StatementPattern (costEstimate=1.3K, resultSizeEstimate=2.6K, resultSizeActual=1.2K, totalTimeActual=0.105ms, selfTimeActual=0.105ms) [left]
//   │     s: Var (name=x1)
//   │     p: Var (name=_const_d85f49f5_uri, value=http://www.wikidata.org/prop/direct/P159, anonymous)
//   │     o: Var (name=_const_7a0b68f5_uri, value=http://www.wikidata.org/entity/Q220, anonymous)
//   └── StatementPattern (costEstimate=1, resultSizeEstimate=81.1K, resultSizeActual=3, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [right]
//         s: Var (name=x1)
//         p: Var (name=_const_e5f28efe_uri, value=http://www.wikidata.org/prop/direct/P31, anonymous)
//         o: Var (name=_const_24e3a460_uri, value=http://www.wikidata.org/entity/Q33506, anonymous)
//
// */
//
//
//
//		}
//
//	}
//
//	@Test
//	public void testNumberOfTennisPlayersFromCountriesInTheEU() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       SELECT * WHERE {
//					         	?x1  wdt:P31  wd:Q5 .
//								?x1  wdt:P106  wd:Q10833314 .
//								?x1  wdt:P19  ?country .
//								?country wdt:P361  wd:Q458 .
//					         }
//					         """;
//
///*
//- number of tennis players from countries in the EU
//
//select (count(?s) as ?c) where
//Eu administrative_regions ?country .
//?s instanceOf Human
//?s occupation TennisPlayers
//?s bornIn ?country
//*/
//
//			QueryJoinOptimizer.MERGE_JOIN_CARDINALITY_SIZE_DIFF_MULTIPLIER = 100000;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
//
//			EndpointTripleSource.ENABLE_MERGE_JOIN = false;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
//
//
//		}
//
//	}
//
//	@Test
//	public void testHumansAndTheirPlaceOfBirth() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       SELECT * WHERE {
//					         	?x1  wdt:P31  wd:Q5 .
//								?x1  wdt:P19  ?country .
//					         }
//					         """;
//
//
//			QueryJoinOptimizer.MERGE_JOIN_CARDINALITY_SIZE_DIFF_MULTIPLIER = 10;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
///*
//Projection (resultSizeActual=2.8M, totalTimeActual=1.6s, selfTimeActual=194ms)
//╠══ ProjectionElemList
//║     ProjectionElem "x1"
//║     ProjectionElem "country"
//╚══ Join (InnerMergeJoinIterator) (resultSizeActual=2.8M, totalTimeActual=1.4s, selfTimeActual=472ms)
//   ├── StatementPattern [statementOrder: S]  (costEstimate=2.8M, resultSizeEstimate=5.6M, resultSizeActual=2.8M, totalTimeActual=241ms, selfTimeActual=241ms) [left]
//   │     s: Var (name=x1)
//   │     p: Var (name=_const_e5f28ec8_uri, value=http://www.wikidata.org/prop/direct/P19, anonymous)
//   │     o: Var (name=country)
//   └── StatementPattern [statementOrder: S]  (costEstimate=1, resultSizeEstimate=18.4M, resultSizeActual=9.1M, totalTimeActual=718ms, selfTimeActual=718ms) [right]
//         s: Var (name=x1)
//         p: Var (name=_const_e5f28efe_uri, value=http://www.wikidata.org/prop/direct/P31, anonymous)
//         o: Var (name=_const_f822a47a_uri, value=http://www.wikidata.org/entity/Q5, anonymous)
//
//*/
//
//			EndpointTripleSource.ENABLE_MERGE_JOIN = false;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
///*
//Projection (resultSizeActual=2.8M, totalTimeActual=7.7s, selfTimeActual=174ms)
//╠══ ProjectionElemList
//║     ProjectionElem "x1"
//║     ProjectionElem "country"
//╚══ Join (JoinIterator) (resultSizeActual=2.8M, totalTimeActual=7.5s, selfTimeActual=7.4s)
//   ├── StatementPattern (costEstimate=2.8M, resultSizeEstimate=5.6M, resultSizeActual=2.8M, totalTimeActual=173ms, selfTimeActual=173ms) [left]
//   │     s: Var (name=x1)
//   │     p: Var (name=_const_e5f28ec8_uri, value=http://www.wikidata.org/prop/direct/P19, anonymous)
//   │     o: Var (name=country)
//   └── StatementPattern (costEstimate=1, resultSizeEstimate=18.4M, resultSizeActual=2.8M, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [right]
//         s: Var (name=x1)
//         p: Var (name=_const_e5f28efe_uri, value=http://www.wikidata.org/prop/direct/P31, anonymous)
//         o: Var (name=_const_f822a47a_uri, value=http://www.wikidata.org/entity/Q5, anonymous)
//
// */
//
//		}
//
//	}
//
//
//	@Test
//	public void testBoardMembersOfFourCompanies() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//
//					       SELECT * WHERE {
//					         	?company1  wdt:P3320  ?boardMember .
//					         	?company2  wdt:P3320  ?boardMember .
//					         	?company3  wdt:P3320  ?boardMember .
//					         	?company4  wdt:P3320  ?boardMember .
//
//					         	filter(?company1 != ?company2 && ?company1 != ?company3 && ?company2 != ?company3 && ?company1 != ?company4 && ?company2 != ?company4 && ?company3 != ?company4)
//
//								?boardMember wdt:P31  ?type ;
//								    wdt:P21 ?sexOrGender;
//								    wdt:P735 ?givenName ;
//								    wdt:P734 ?familyName ;
//								    wdt:P106 ?occupation ;
//								    wdt:P27 ?countryOfCitizenship .
//
//					         }
//					         """;
//
//
//			QueryJoinOptimizer.MERGE_JOIN_CARDINALITY_SIZE_DIFF_MULTIPLIER = 10;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
///*
//Projection (resultSizeActual=2.6K, totalTimeActual=38.8ms, selfTimeActual=0.309ms)
//╠══ ProjectionElemList
//║     ProjectionElem "company1"
//║     ProjectionElem "boardMember"
//║     ProjectionElem "company2"
//║     ProjectionElem "company3"
//║     ProjectionElem "company4"
//║     ProjectionElem "type"
//║     ProjectionElem "sexOrGender"
//║     ProjectionElem "givenName"
//║     ProjectionElem "familyName"
//║     ProjectionElem "occupation"
//║     ProjectionElem "countryOfCitizenship"
//╚══ Join (JoinIterator) (resultSizeActual=2.6K, totalTimeActual=38.5ms, selfTimeActual=32.8ms)
//   ├── Filter (resultSizeActual=7.2K, totalTimeActual=5.67ms, selfTimeActual=1.21ms) [left]
//   │  ╠══ And
//   │  ║  ├── And
//   │  ║  │  ╠══ Compare (!=)
//   │  ║  │  ║     Var (name=company1)
//   │  ║  │  ║     Var (name=company4)
//   │  ║  │  ╚══ Compare (!=)
//   │  ║  │        Var (name=company2)
//   │  ║  │        Var (name=company4)
//   │  ║  └── Compare (!=)
//   │  ║        Var (name=company3)
//   │  ║        Var (name=company4)
//   │  ╚══ Join (InnerMergeJoinIterator) (resultSizeActual=13.2K, totalTimeActual=4.46ms, selfTimeActual=1.48ms)
//   │     ├── Filter (resultSizeActual=1.9K, totalTimeActual=2.64ms, selfTimeActual=0.31ms) [left]
//   │     │  ╠══ And
//   │     │  ║  ├── Compare (!=)
//   │     │  ║  │     Var (name=company1)
//   │     │  ║  │     Var (name=company3)
//   │     │  ║  └── Compare (!=)
//   │     │  ║        Var (name=company2)
//   │     │  ║        Var (name=company3)
//   │     │  ╚══ Join (InnerMergeJoinIterator) (resultSizeActual=3.9K, totalTimeActual=2.33ms, selfTimeActual=0.615ms)
//   │     │     ├── Filter (resultSizeActual=988, totalTimeActual=1.41ms, selfTimeActual=0.183ms) [left]
//   │     │     │  ╠══ Compare (!=)
//   │     │     │  ║     Var (name=company1)
//   │     │     │  ║     Var (name=company2)
//   │     │     │  ╚══ Join (InnerMergeJoinIterator) (resultSizeActual=3.7K, totalTimeActual=1.23ms, selfTimeActual=0.546ms)
//   │     │     │     ├── StatementPattern [statementOrder: O]  (costEstimate=545, resultSizeEstimate=5.4K, resultSizeActual=2.7K, totalTimeActual=0.334ms, selfTimeActual=0.334ms) [left]
//   │     │     │     │     s: Var (name=company1)
//   │     │     │     │     p: Var (name=_const_338ad53e_uri, value=http://www.wikidata.org/prop/direct/P3320, anonymous)
//   │     │     │     │     o: Var (name=boardMember)
//   │     │     │     └── StatementPattern [statementOrder: O]  (costEstimate=74, resultSizeEstimate=5.4K, resultSizeActual=2.7K, totalTimeActual=0.348ms, selfTimeActual=0.348ms) [right]
//   │     │     │           s: Var (name=company2)
//   │     │     │           p: Var (name=_const_338ad53e_uri, value=http://www.wikidata.org/prop/direct/P3320, anonymous)
//   │     │     │           o: Var (name=boardMember)
//   │     │     └── StatementPattern [statementOrder: O]  (costEstimate=74, resultSizeEstimate=5.4K, resultSizeActual=2.7K, totalTimeActual=0.299ms, selfTimeActual=0.299ms) [right]
//   │     │           s: Var (name=company3)
//   │     │           p: Var (name=_const_338ad53e_uri, value=http://www.wikidata.org/prop/direct/P3320, anonymous)
//   │     │           o: Var (name=boardMember)
//   │     └── StatementPattern [statementOrder: O]  (costEstimate=74, resultSizeEstimate=5.4K, resultSizeActual=2.7K, totalTimeActual=0.343ms, selfTimeActual=0.343ms) [right]
//   │           s: Var (name=company4)
//   │           p: Var (name=_const_338ad53e_uri, value=http://www.wikidata.org/prop/direct/P3320, anonymous)
//   │           o: Var (name=boardMember)
//   └── Join (JoinIterator) (resultSizeActual=2.6K, totalTimeActual=0.0ms) [right]
//      ╠══ StatementPattern (costEstimate=2.6K, resultSizeEstimate=6.6M, resultSizeActual=6.6K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//      ║     s: Var (name=boardMember)
//      ║     p: Var (name=_const_d85f6038_uri, value=http://www.wikidata.org/prop/direct/P734, anonymous)
//      ║     o: Var (name=familyName)
//      ╚══ Join (JoinIterator) (resultSizeActual=2.6K, totalTimeActual=0.004ms, selfTimeActual=0.001ms) [right]
//         ├── StatementPattern (costEstimate=2.9K, resultSizeEstimate=8.2M, resultSizeActual=6.7K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//         │     s: Var (name=boardMember)
//         │     p: Var (name=_const_e5f28ee5_uri, value=http://www.wikidata.org/prop/direct/P27, anonymous)
//         │     o: Var (name=countryOfCitizenship)
//         └── Join (JoinIterator) (resultSizeActual=2.6K, totalTimeActual=0.003ms, selfTimeActual=0.001ms) [right]
//            ╠══ StatementPattern (costEstimate=3.5K, resultSizeEstimate=12.1M, resultSizeActual=6.7K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//            ║     s: Var (name=boardMember)
//            ║     p: Var (name=_const_d85f6039_uri, value=http://www.wikidata.org/prop/direct/P735, anonymous)
//            ║     o: Var (name=givenName)
//            ╚══ Join (JoinIterator) (resultSizeActual=2.6K, totalTimeActual=0.003ms, selfTimeActual=0.001ms) [right]
//               ├── StatementPattern (costEstimate=3.8K, resultSizeEstimate=14.6M, resultSizeActual=6.7K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//               │     s: Var (name=boardMember)
//               │     p: Var (name=_const_e5f28edf_uri, value=http://www.wikidata.org/prop/direct/P21, anonymous)
//               │     o: Var (name=sexOrGender)
//               └── Join (JoinIterator) (resultSizeActual=2.6K, totalTimeActual=0.002ms, selfTimeActual=0.001ms) [right]
//                  ╠══ StatementPattern (costEstimate=4.1K, resultSizeEstimate=17.1M, resultSizeActual=2.6K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//                  ║     s: Var (name=boardMember)
//                  ║     p: Var (name=_const_d85f4957_uri, value=http://www.wikidata.org/prop/direct/P106, anonymous)
//                  ║     o: Var (name=occupation)
//                  ╚══ StatementPattern (costEstimate=13.8K, resultSizeEstimate=191.5M, resultSizeActual=2.6K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [right]
//                        s: Var (name=boardMember)
//                        p: Var (name=_const_e5f28efe_uri, value=http://www.wikidata.org/prop/direct/P31, anonymous)
//                        o: Var (name=type)
//
// */
//
//			EndpointTripleSource.ENABLE_MERGE_JOIN = false;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
///*
//Projection (resultSizeActual=2.6K, totalTimeActual=163ms, selfTimeActual=0.316ms)
//╠══ ProjectionElemList
//║     ProjectionElem "company1"
//║     ProjectionElem "boardMember"
//║     ProjectionElem "company2"
//║     ProjectionElem "company3"
//║     ProjectionElem "company4"
//║     ProjectionElem "type"
//║     ProjectionElem "sexOrGender"
//║     ProjectionElem "givenName"
//║     ProjectionElem "familyName"
//║     ProjectionElem "occupation"
//║     ProjectionElem "countryOfCitizenship"
//╚══ Filter (resultSizeActual=2.6K, totalTimeActual=163ms, selfTimeActual=0.54ms)
//   ├── And
//   │  ╠══ And
//   │  ║  ├── Compare (!=)
//   │  ║  │     Var (name=company1)
//   │  ║  │     Var (name=company2)
//   │  ║  └── Compare (!=)
//   │  ║        Var (name=company1)
//   │  ║        Var (name=company3)
//   │  ╚══ Compare (!=)
//   │        Var (name=company1)
//   │        Var (name=company4)
//   └── Join (JoinIterator) (resultSizeActual=7.0K, totalTimeActual=163ms, selfTimeActual=162ms)
//      ╠══ StatementPattern (costEstimate=545, resultSizeEstimate=5.4K, resultSizeActual=2.7K, totalTimeActual=0.416ms, selfTimeActual=0.416ms) [left]
//      ║     s: Var (name=company1)
//      ║     p: Var (name=_const_338ad53e_uri, value=http://www.wikidata.org/prop/direct/P3320, anonymous)
//      ║     o: Var (name=boardMember)
//      ╚══ Filter (resultSizeActual=7.0K, totalTimeActual=0.073ms, selfTimeActual=0.0ms) [right]
//         ├── And
//         │  ╠══ Compare (!=)
//         │  ║     Var (name=company2)
//         │  ║     Var (name=company3)
//         │  ╚══ Compare (!=)
//         │        Var (name=company2)
//         │        Var (name=company4)
//         └── Join (JoinIterator) (resultSizeActual=14.3K, totalTimeActual=0.073ms, selfTimeActual=0.001ms)
//            ╠══ StatementPattern (costEstimate=74, resultSizeEstimate=5.4K, resultSizeActual=3.7K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//            ║     s: Var (name=company2)
//            ║     p: Var (name=_const_338ad53e_uri, value=http://www.wikidata.org/prop/direct/P3320, anonymous)
//            ║     o: Var (name=boardMember)
//            ╚══ Filter (resultSizeActual=14.3K, totalTimeActual=0.071ms, selfTimeActual=0.0ms) [right]
//               ├── Compare (!=)
//               │     Var (name=company3)
//               │     Var (name=company4)
//               └── Join (JoinIterator) (resultSizeActual=22.5K, totalTimeActual=0.071ms, selfTimeActual=0.001ms)
//                  ╠══ StatementPattern (costEstimate=74, resultSizeEstimate=5.4K, resultSizeActual=7.6K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//                  ║     s: Var (name=company3)
//                  ║     p: Var (name=_const_338ad53e_uri, value=http://www.wikidata.org/prop/direct/P3320, anonymous)
//                  ║     o: Var (name=boardMember)
//                  ╚══ Join (JoinIterator) (resultSizeActual=22.5K, totalTimeActual=0.07ms, selfTimeActual=0.07ms) [right]
//                     ├── StatementPattern (costEstimate=74, resultSizeEstimate=5.4K, resultSizeActual=28.8K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//                     │     s: Var (name=company4)
//                     │     p: Var (name=_const_338ad53e_uri, value=http://www.wikidata.org/prop/direct/P3320, anonymous)
//                     │     o: Var (name=boardMember)
//                     └── Join (JoinIterator) (resultSizeActual=22.5K, totalTimeActual=0.0ms) [right]
//                        ╠══ StatementPattern (costEstimate=2.6K, resultSizeEstimate=6.6M, resultSizeActual=22.2K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//                        ║     s: Var (name=boardMember)
//                        ║     p: Var (name=_const_d85f6038_uri, value=http://www.wikidata.org/prop/direct/P734, anonymous)
//                        ║     o: Var (name=familyName)
//                        ╚══ Join (JoinIterator) (resultSizeActual=22.5K, totalTimeActual=0.004ms, selfTimeActual=0.001ms) [right]
//                           ├── StatementPattern (costEstimate=2.9K, resultSizeEstimate=8.2M, resultSizeActual=22.5K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//                           │     s: Var (name=boardMember)
//                           │     p: Var (name=_const_e5f28ee5_uri, value=http://www.wikidata.org/prop/direct/P27, anonymous)
//                           │     o: Var (name=countryOfCitizenship)
//                           └── Join (JoinIterator) (resultSizeActual=22.5K, totalTimeActual=0.003ms, selfTimeActual=0.001ms) [right]
//                              ╠══ StatementPattern (costEstimate=3.5K, resultSizeEstimate=12.1M, resultSizeActual=23.0K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//                              ║     s: Var (name=boardMember)
//                              ║     p: Var (name=_const_d85f6039_uri, value=http://www.wikidata.org/prop/direct/P735, anonymous)
//                              ║     o: Var (name=givenName)
//                              ╚══ Join (JoinIterator) (resultSizeActual=22.5K, totalTimeActual=0.002ms, selfTimeActual=0.001ms) [right]
//                                 ├── StatementPattern (costEstimate=3.8K, resultSizeEstimate=14.6M, resultSizeActual=23.0K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//                                 │     s: Var (name=boardMember)
//                                 │     p: Var (name=_const_e5f28edf_uri, value=http://www.wikidata.org/prop/direct/P21, anonymous)
//                                 │     o: Var (name=sexOrGender)
//                                 └── Join (JoinIterator) (resultSizeActual=22.5K, totalTimeActual=0.001ms, selfTimeActual=0.001ms) [right]
//                                    ╠══ StatementPattern (costEstimate=4.1K, resultSizeEstimate=17.1M, resultSizeActual=22.5K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [left]
//                                    ║     s: Var (name=boardMember)
//                                    ║     p: Var (name=_const_d85f4957_uri, value=http://www.wikidata.org/prop/direct/P106, anonymous)
//                                    ║     o: Var (name=occupation)
//                                    ╚══ StatementPattern (costEstimate=13.8K, resultSizeEstimate=191.5M, resultSizeActual=22.5K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [right]
//                                          s: Var (name=boardMember)
//                                          p: Var (name=_const_e5f28efe_uri, value=http://www.wikidata.org/prop/direct/P31, anonymous)
//                                          o: Var (name=type)
//
// */
//
//		}
//
//	}
//
//
//	@Test
//	public void testTemp() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//					PREFIX schema: <http://schema.org/>
//     				PREFIX dct: <http://purl.org/dc/terms/>
//     				PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
//PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
//PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>
//PREFIX pr: <http://www.wikidata.org/prop/reference/>
//PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
//PREFIX prov: <http://www.w3.org/ns/prov#>
//
//
//#two chemical compounds with the same CAS registry number
//                                  SELECT DISTINCT ?cas ?compound1 ?compound1Label ?compound2 ?compound2Label WHERE {
//                                    ?compound1 wdt:P231 ?cas .
//                                    ?compound2 wdt:P231 ?cas .
//                                    FILTER (?compound1 != ?compound2)
//                                  }
//					         """;
//
//
//			QueryJoinOptimizer.MERGE_JOIN_CARDINALITY_SIZE_DIFF_MULTIPLIER = 10;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
///*
//Distinct (resultSizeActual=860, totalTimeActual=351ms, selfTimeActual=2.81ms)
//   Projection (resultSizeActual=860, totalTimeActual=348ms, selfTimeActual=0.133ms)
//   ├── ProjectionElemList
//   │     ProjectionElem "cas"
//   │     ProjectionElem "compound1"
//   │     ProjectionElem "compound1Label"
//   │     ProjectionElem "compound2"
//   │     ProjectionElem "compound2Label"
//   └── Filter (resultSizeActual=860, totalTimeActual=348ms, selfTimeActual=31.3ms)
//      ╠══ Compare (!=)
//      ║     Var (name=compound1)
//      ║     Var (name=compound2)
//      ╚══ Join (InnerMergeJoinIterator) (resultSizeActual=932.4K, totalTimeActual=316ms, selfTimeActual=147ms)
//         ├── StatementPattern [statementOrder: O]  (costEstimate=931.6K, resultSizeEstimate=1.9M, resultSizeActual=931.5K, totalTimeActual=94.3ms, selfTimeActual=94.3ms) [left]
//         │     s: Var (name=compound1)
//         │     p: Var (name=_const_d85f4d70_uri, value=http://www.wikidata.org/prop/direct/P231, anonymous)
//         │     o: Var (name=cas)
//         └── StatementPattern [statementOrder: O]  (costEstimate=1.4K, resultSizeEstimate=1.9M, resultSizeActual=931.5K, totalTimeActual=74.8ms, selfTimeActual=74.8ms) [right]
//               s: Var (name=compound2)
//               p: Var (name=_const_d85f4d70_uri, value=http://www.wikidata.org/prop/direct/P231, anonymous)
//               o: Var (name=cas)
//
// */
//
//			EndpointTripleSource.ENABLE_MERGE_JOIN = false;
//			runQuery(connection, query);
//			System.out.println(runQuery(connection, query));
//
///*
//Distinct (resultSizeActual=860, totalTimeActual=1.2s, selfTimeActual=2.47ms)
//   Projection (resultSizeActual=860, totalTimeActual=1.2s, selfTimeActual=0.203ms)
//   ├── ProjectionElemList
//   │     ProjectionElem "cas"
//   │     ProjectionElem "compound1"
//   │     ProjectionElem "compound1Label"
//   │     ProjectionElem "compound2"
//   │     ProjectionElem "compound2Label"
//   └── Filter (resultSizeActual=860, totalTimeActual=1.2s, selfTimeActual=25.8ms)
//      ╠══ Compare (!=)
//      ║     Var (name=compound1)
//      ║     Var (name=compound2)
//      ╚══ Join (JoinIterator) (resultSizeActual=932.4K, totalTimeActual=1.2s, selfTimeActual=1.1s)
//         ├── StatementPattern (costEstimate=931.6K, resultSizeEstimate=1.9M, resultSizeActual=931.5K, totalTimeActual=80.2ms, selfTimeActual=80.2ms) [left]
//         │     s: Var (name=compound1)
//         │     p: Var (name=_const_d85f4d70_uri, value=http://www.wikidata.org/prop/direct/P231, anonymous)
//         │     o: Var (name=cas)
//         └── StatementPattern (costEstimate=1.4K, resultSizeEstimate=1.9M, resultSizeActual=932.4K, totalTimeActual=0.0ms, selfTimeActual=0.0ms) [right]
//               s: Var (name=compound2)
//               p: Var (name=_const_d85f4d70_uri, value=http://www.wikidata.org/prop/direct/P231, anonymous)
//               o: Var (name=cas)
//
// */
//
//		}
//
//	}
//
//
//	@Test
//	public void testTemp2() {
//		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
//			System.out.println();
//			String query = """
//					PREFIX wd: <http://www.wikidata.org/entity/>
//					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
//					PREFIX wikibase: <http://wikiba.se/ontology#>
//					PREFIX p: <http://www.wikidata.org/prop/>
//					PREFIX ps: <http://www.wikidata.org/prop/statement/>
//					PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
//					PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//					PREFIX bd: <http://www.bigdata.com/rdf#>
//					PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
//					PREFIX schema: <http://schema.org/>
//     				PREFIX dct: <http://purl.org/dc/terms/>
//     				PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
//PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
//PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>
//PREFIX pr: <http://www.wikidata.org/prop/reference/>
//PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
//PREFIX prov: <http://www.w3.org/ns/prov#>
//
//#defaultView:BubbleChart
//SELECT *
//WHERE
//{
//        ?object wdt:P50 ?author .
//        ?author wdt:P106 ?occupation .
//}
//
//					         """;
//
//
//			QueryJoinOptimizer.MERGE_JOIN_CARDINALITY_SIZE_DIFF_MULTIPLIER = 100;
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
////			runQuery(connection, query);
//
////			QueryJoinOptimizer.MERGE_JOIN_CARDINALITY_SIZE_DIFF_MULTIPLIER = 10;
////			runQuery(connection, query);
//
///*
//
// */
////
////			EndpointTripleSource.ENABLE_MERGE_JOIN = false;
////			runQuery(connection, query);
//
///*
//
// */
//
//		}
//
//	}
//
//
//
//	private static String runQuery(SailRepositoryConnection connection, String query) {
//		StopWatch stopWatch = StopWatch.createStarted();
//		TupleQuery tupleQuery = connection.prepareTupleQuery(query);
//		tupleQuery.setMaxExecutionTime(10*60);
//		Explanation explain = tupleQuery.explain(Explanation.Level.Timed);
////		System.out.println(explain);
////		System.out.println();
//		System.out.println("Took: " + stopWatch.formatTime());
//
//		return explain.toString();
//
//	}
//}

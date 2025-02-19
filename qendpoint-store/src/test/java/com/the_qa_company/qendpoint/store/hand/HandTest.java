package com.the_qa_company.qendpoint.store.hand;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.the_qa_company.qendpoint.compiler.CompiledSail;
import com.the_qa_company.qendpoint.compiler.SparqlRepository;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.utils.rdf.ClosableResult;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class HandTest {
	@TempDir
	public Path tempDir;

	@Test
	public void largeTest() throws IOException {
		Path root = Path.of("C:\\Users\\wilat\\workspace\\qEndpoint\\qendpoint\\hdt-store\\wdbench-qep");
		String index = "wdbench.hdt";

		EndpointFiles files = new EndpointFiles(root, index);

		StopWatch sw = new StopWatch();
		try (HDT hdt = HDTManager.mapIndexedHDT(files.getHDTIndexPath())) {
			System.out.println("map: " + sw.stopAndShow());
			sw.reset();

			final String wdt = "http://www.wikidata.org/prop/direct/";
			final String wd = "http://www.wikidata.org/entity/";

			Dictionary d = hdt.getDictionary();
			long p31 = d.stringToId(wdt + "P31", TripleComponentRole.PREDICATE);
			long q5 = d.stringToId(wd + "Q5", TripleComponentRole.OBJECT);

			System.out.println("start: " + sw.stopAndShow());
			sw.reset();

			IteratorTripleID it = hdt.getTriples().search(new TripleID(0, p31, q5));

			long h = 0;
			long count = 0;

			Object[] ds = new Object[100_000_000];

			while (it.hasNext()) {
				TripleID tid = it.next();
				long position = it.getLastTriplePosition();

				for (int i = 0; i < 8; i++) {
					ds[(int) count * 8 + i] = new Object();
				}
				/*
				 * reuse objects? run rdf4j -> see objects created check with a
				 * sort if we keep objects
				 */

				count++;
			}

			System.out.println("done " + count + " in " + sw.stopAndShow() + " / " + ds.length);
			System.out.println("/" + Objects.hash(ds));
			sw.reset();
		}

	}

	@Test
	public void large2Test() throws IOException, InterruptedException {
		Path root = Path.of("C:\\Users\\wilat\\workspace\\qEndpoint\\qendpoint\\hdt-store\\wdbench-qep");
		String index = "wdbench.hdt";

		EndpointFiles files = new EndpointFiles(root, index);

		StopWatch sw = new StopWatch();

		SparqlRepository sparql = CompiledSail.compiler().withEndpointFiles(files).compileToSparqlRepository();
		sparql.init();

		System.out.println("init in " + sw.stopAndShow());

		Thread.sleep(20_000);

		try {
			sw.reset();
			try (ClosableResult<TupleQueryResult> res = sparql.executeTupleQuery("""
					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
					PREFIX wd: <http://www.wikidata.org/entity/>

					SELECT (count(?s) as ?c) {
					   ?s wdt:P31 wd:Q5 .
					}
					""", -1)) {
				res.getResult().forEach(System.out::println);
				System.out.println("done in " + sw.stopAndShow());
			}

		} finally {
			sparql.shutDown();
		}

	}

	public void luceneDumpTest(IndexReader dr) throws IOException {
		Path fs = Path.of("C:\\Users\\wilat\\workspace\\qEndpoint\\qendpoint\\geosparql\\qendpointlucene\\geo");
		Path out = Path.of("C:\\Users\\wilat\\workspace\\qEndpoint\\qendpoint\\geosparql\\qendpointlucene_geo.json");

		JsonFactory fact = new JsonFactory();
		try (JsonGenerator gen = fact.createGenerator(out.toFile(), JsonEncoding.UTF8)) {
			gen.setPrettyPrinter(new DefaultPrettyPrinter());
			int docs = dr.numDocs();

			gen.writeStartObject();
			gen.writeArrayFieldStart("docs");
			for (int i = 0; i < docs; i++) {
				Document doc = dr.document(i);
				gen.writeStartObject();
				gen.writeNumberField("id", i);
				gen.writeArrayFieldStart("fields");
				List<IndexableField> fields = doc.getFields();
				for (IndexableField field : fields) {
					gen.writeStartObject();
					gen.writeStringField("name", field.name());
					gen.writeStringField("type", field.fieldType().toString());
					gen.writeStringField("value", field.stringValue());
					gen.writeEndObject();
				}
				gen.writeEndArray();
				gen.writeEndObject();
			}
			gen.writeEndArray();
			gen.writeEndObject();

		}
		System.out.println("Dump into " + out);

	}

	private void printHead(TupleQueryResult res) {
		for (int i = 0; i < 5; i++) {
			if (!res.hasNext()) {
				return;
			}
			System.out.println(res.next());
		}
		if (!res.hasNext()) {
			return;
		}
		int more = 0;
		while (res.hasNext()) {
			more++;
			res.next();
		}
		System.out.println("... +" + more);
	}

	private void geoSparqlTest(boolean useLucene) throws IOException {
		NotifyingSail store = new MemoryStore();
		if (useLucene) {
			Path qep = Path.of("C:\\Users\\wilat\\workspace\\qEndpoint\\qendpoint\\geosparql\\qendpoint");
			LuceneSail lc = new LuceneSail();
			lc.setBaseSail(store);
			lc.setDataDir(qep.resolve("lucene").toFile());
			lc.setParameter(LuceneSail.WKT_FIELDS,
					"https://linkedopendata.eu/prop/direct/P127 http://nuts.de/geometry");
			lc.setParameter(LuceneSail.MAX_DOCUMENTS_KEY, "5000");
			lc.setEvaluationMode(TupleFunctionEvaluationMode.NATIVE);
			store = lc;
		}
		System.out.println("Lucene: " + useLucene);
		SailRepository repo = new SailRepository(store);

		try {
			Repositories.consume(repo, RepositoryConnection::clear);

			Repositories.consume(repo, conn -> {
				try {
					conn.add(new File("C:\\Users\\wilat\\workspace\\qep\\geo2.ttl"));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});

			// repo.reindexLuceneSails();
			if (store instanceof LuceneSail lss) {
				lss.reindex();
				// if (!(lss.getLuceneIndex() instanceof LuceneIndex li)) {
				// throw new AssertionError();
				// }
				// luceneDumpTest(li.getIndexReader());
			}

			try (SailRepositoryConnection conn = repo.getConnection()) {
				{
					TupleQuery tq = conn.prepareTupleQuery(
							"""
									PREFIX geo: <http://www.opengis.net/ont/geosparql#>
									PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
									PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
									PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
									SELECT * {
										?project <https://linkedopendata.eu/prop/direct/P127> ?coords.\s
										FILTER(<http://www.opengis.net/def/function/geosparql/ehContains>("POLYGON((6.822266 46.635784, 8.822265999999999 46.635784, 8.822265999999999 48.635784, 6.822266 48.635784, 6.822266 46.635784))"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, ?coords ))
									}
											""");

					System.out.println("Q1:");
					try (TupleQueryResult res = tq.evaluate()) {
						printHead(res);
					}
					System.out.println("---------------");
				}
				{
					TupleQuery tq = conn.prepareTupleQuery(
							"""
									PREFIX geo: <http://www.opengis.net/ont/geosparql#>
									PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
									PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
									PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
									SELECT ?id ?o WHERE {
													?s rdf:type <http://nuts.de/NUTS3> .
													?s <http://nuts.de/geometry> ?o .
													FILTER (geof:ehContains("POINT(5.1578829866667 45.596920046667)"^^geo:wktLiteral,?o))
													?s <http://nuts.de/id> ?id .
									  }
											""");

					System.out.println("Q2:");
					try (TupleQueryResult res = tq.evaluate()) {
						printHead(res);
					}
					System.out.println("---------------");
				}
				{
					TupleQuery tq = conn.prepareTupleQuery(
							"""
									SELECT DISTINCT ?coordinates ?infoRegioID
									WHERE {
									  {
									    SELECT ?s0 ((<http://www.opengis.net/def/function/geosparql/distance>("POINT(4.482422 45.521744)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, ?coordinates, <http://www.opengis.net/def/uom/OGC/1.0/metre>)) AS ?distance)
									    WHERE {
									      ?s0 <https://linkedopendata.eu/prop/direct/P35> <https://linkedopendata.eu/entity/Q9934> .
									      ?s0 <https://linkedopendata.eu/prop/direct/P127> ?coordinates .
									      FILTER (<http://www.opengis.net/def/function/geosparql/distance>("POINT(4.482422 45.521744)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, ?coordinates, <http://www.opengis.net/def/uom/OGC/1.0/metre>) < 100000)
									    }
									    ORDER BY ?distance
									    LIMIT 1000
									  }
									  ?s0 <https://linkedopendata.eu/prop/direct/P127> ?coordinates .
									  OPTIONAL {
									    ?s0 <https://linkedopendata.eu/prop/direct/P1741> ?infoRegioID .
									  }
									  FILTER ((<http://www.opengis.net/def/function/geosparql/distance>("POINT(4.482422 45.521744)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, ?coordinates, <http://www.opengis.net/def/uom/OGC/1.0/metre>)) < 100000)
									}
											""");

					System.out.println("Q3:");
					try (TupleQueryResult res = tq.evaluate()) {
						printHead(res);
					}
					System.out.println("---------------");
				}
			}
		} finally {
			repo.shutDown();
		}
	}

	@Test
	public void geoSparqlTest() throws IOException {
		geoSparqlTest(true);
		geoSparqlTest(false);
	}

	private void queryLitTestQuery(SparqlRepository sparql, String query, String expected) {
		try (ClosableResult<TupleQueryResult> res = sparql.executeTupleQuery(query, -1)) {
			TupleQueryResult r = res.getResult();
			assertTrue(r.hasNext());
			assertEquals(r.next().getValue("s"), Values.iri(expected));
		}
	}

	@Test
	public void queryLitTest() throws IOException, ParserException {
		String dataset = """
				<http://example.org/#test> <http://example.org/#pred> ""^^<http://www.w3.org/2001/XMLSchema#string> .
				<http://example.org/#test> <http://example.org/#pred> "" .
				<http://example.org/#test> <http://example.org/#pred> "65"^^<http://www.w3.org/2001/XMLSchema#string> .
				<http://example.org/#test> <http://example.org/#pred> "12" .
				<http://example.org/#test> <http://example.org/#pred> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
				<http://example.org/#test> <http://example.org/#pred> "hola muchachos"@es .
				<http://example.org/#test> <http://example.org/#pred> <http://example.org/#test2> .
				""";

		Path root = tempDir.resolve("qep");
		Files.createDirectories(root);
		EndpointFiles files = new EndpointFiles(root);

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);
		try (HDT hdt = HDTManager.generateHDT(new ByteArrayInputStream(dataset.getBytes(StandardCharsets.UTF_8)),
				"file:///test.ttl", "test.ttl", spec, ProgressListener.sout())) {
			Path path = files.getHDTIndexPath();
			Files.createDirectories(path.getParent());
			hdt.saveToHDT(path);
		}

		SparqlRepository sparql = CompiledSail.compiler().withEndpointFiles(files).compileToSparqlRepository();

		try {
			queryLitTestQuery(sparql, "SELECT ?s { ?s <http://example.org/#pred> '' . }", "http://example.org/#test");
			queryLitTestQuery(sparql, "SELECT ?s { ?s <http://example.org/#pred> '65' . }", "http://example.org/#test");
			queryLitTestQuery(sparql, "SELECT ?s { ?s <http://example.org/#pred> '12' . }", "http://example.org/#test");
			queryLitTestQuery(sparql, "SELECT ?s { ?s <http://example.org/#pred> 'hola muchachos'@es . }",
					"http://example.org/#test");
		} finally {
			sparql.shutDown();
		}
	}
}

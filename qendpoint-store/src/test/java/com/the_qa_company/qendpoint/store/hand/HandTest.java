package com.the_qa_company.qendpoint.store.hand;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.the_qa_company.qendpoint.compiler.CompiledSail;
import com.the_qa_company.qendpoint.compiler.SparqlRepository;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.utils.rdf.ClosableResult;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.transaction.QueryEvaluationMode;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.BinaryValueOperator;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryValueEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategyFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.QueryPrologLexer;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLQueries;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.text.csv.SPARQLResultsCSVWriter;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
			long p131 = d.stringToId(wdt + "P131", TripleComponentRole.PREDICATE);
			// human
			long q5 = d.stringToId(wd + "Q5", TripleComponentRole.OBJECT);
			// museum
			long q33506 = d.stringToId(wd + "Q33506", TripleComponentRole.OBJECT);
			// roma
			long q220 = d.stringToId(wd + "Q220", TripleComponentRole.OBJECT);
			// roma
			long p106 = d.stringToId(wdt + "P106", TripleComponentRole.PREDICATE);

			System.out.println("start: " + sw.stopAndShow());
			sw.reset();
			{

				IteratorTripleID it = hdt.getTriples().search(new TripleID(0, p31, q5));

				long h = 0;
				long count = 0;

				Object[] ds = new Object[100_000_000];

				while (it.hasNext()) {
					TripleID tid = it.next();
					// long position = it.getLastTriplePosition();

					// for (int i = 0; i < 8; i++) {
					// ds[(int) count * 8 + i] = new Object();
					// }
					/*
					 * reuse objects? run rdf4j -> see objects created check
					 * with a sort if we keep objects
					 */

					count++;
				}

				System.out.println("done " + count + " in " + sw.stopAndShow() + " / " + ds.length);
				System.out.println("/" + Objects.hash(ds));
				sw.reset();
			}
			System.out.println("start: " + sw.stopAndShow());
			sw.reset();
			{

				IteratorTripleID it1 = hdt.getTriples().search(new TripleID(0, p131, q220));

				long h = 0;
				long count = 0;

				Object[] ds = new Object[100_000_000];

				while (it1.hasNext()) {
					TripleID tid = it1.next();

					IteratorTripleID it2 = hdt.getTriples().search(new TripleID(tid.getSubject(), p31, q33506));

					while (it2.hasNext()) {
						TripleID tid2 = it2.next();

						// long position = it.getLastTriplePosition();

						// for (int i = 0; i < 8; i++) {
						// ds[(int) count * 8 + i] = new Object();
						// }
						/*
						 * reuse objects? run rdf4j -> see objects created check
						 * with a sort if we keep objects
						 */

						count++;
					}
				}

				System.out.println("done " + count + " in " + sw.stopAndShow() + " / " + ds.length);
				System.out.println("/" + Objects.hash(ds));
				sw.reset();
			}
			System.out.println("start: " + sw.stopAndShow());
			sw.reset();
			{

				IteratorTripleID it1 = hdt.getTriples().search(new TripleID(0, p31, q5));

				long h = 0;
				long count = 0;

				Object[] ds = new Object[100_000_000];

				while (it1.hasNext()) {
					TripleID tid = it1.next();

					IteratorTripleID it2 = hdt.getTriples().search(new TripleID(tid.getSubject(), p106, 0));

					while (it2.hasNext()) {
						TripleID tid2 = it2.next();

						// long position = it.getLastTriplePosition();

						// for (int i = 0; i < 8; i++) {
						// ds[(int) count * 8 + i] = new Object();
						// }
						/*
						 * reuse objects? run rdf4j -> see objects created check
						 * with a sort if we keep objects
						 */

						count++;
					}
				}

				System.out.println("done " + count + " in " + sw.stopAndShow() + " / " + ds.length);
				System.out.println("/" + Objects.hash(ds));
				sw.reset();
			}

		}

	}

	@Test
	public void large2Test() throws IOException, InterruptedException {
		Path root = Path.of("C:\\Users\\wilat\\workspace\\qEndpoint\\qendpoint\\hdt-store\\wdbench-qep");
		String index = "wdbench.hdt";

		EndpointFiles files = new EndpointFiles(root, index);

		StopWatch sw = new StopWatch();

		SparqlRepository sparql = CompiledSail.compiler().withEndpointFiles(files)
				.withHDTSpec(EndpointStore.OPTION_QENDPOINT_MERGE_JOIN + "=true;").compileToSparqlRepository();
		sparql.init();

		System.out.println("init in " + sw.stopAndShow());

		// Thread.sleep(20_000);

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
			} // 2 sec 503 ms 507 us / 470 ms 993
			sw.reset();
			try (ClosableResult<TupleQueryResult> res = sparql.executeTupleQuery("""
					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
					PREFIX wd: <http://www.wikidata.org/entity/>
					# Q220 roma
					# Q33506 museum
					# P131: "in" P31: a

					SELECT (count(?s) as ?c) {
					                   ?s wdt:P31 wd:Q33506 .
					                   ?s wdt:P131 wd:Q220 .
					}
					""", -1)) {
				res.getResult().forEach(System.out::println);
				System.out.println("done in " + sw.stopAndShow());
			} // 1 sec 521 ms 140 us / 112 ms 134 us

			sw.reset();
			ByteArrayOutputStream ba = new ByteArrayOutputStream();
			sparql.execute("""
					#get_plan
					PREFIX wdt: <http://www.wikidata.org/prop/direct/>
					PREFIX wd: <http://www.wikidata.org/entity/>

					SELECT (COUNT(?profession_id) AS ?count) WHERE {
						?person_id wdt:P31 wd:Q5 .
						?person_id wdt:P106 ?profession_id .
					}
					""", -1, TupleQueryResultFormat.JSON.getDefaultMIMEType(), s -> System.out.println("mime: " + s),
					ba);
			System.out.println(ba.toString(StandardCharsets.UTF_8));
			System.out.println("done in " + sw.stopAndShow());
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

	@Test
	public void optimizerTest() {
		final String exp = "http://example.org/#";
		MemoryStore ms = new MemoryStore();
		SimpleValueFactory vf = SimpleValueFactory.getInstance();
		try (NotifyingSailConnection conn = ms.getConnection()) {
			conn.begin();
			conn.addStatement(vf.createIRI(exp + "test42"), vf.createIRI(exp + "p"), vf.createLiteral(42));
			conn.addStatement(vf.createIRI(exp + "test42"), vf.createIRI(exp + "id"), vf.createLiteral("hello 42"));
			conn.addStatement(vf.createIRI(exp + "test34"), vf.createIRI(exp + "p"), vf.createLiteral(34));
			conn.addStatement(vf.createIRI(exp + "test34"), vf.createIRI(exp + "id"), vf.createLiteral("hello 34"));
			conn.addStatement(vf.createIRI(exp + "test12"), vf.createIRI(exp + "p"), vf.createLiteral(12));
			conn.addStatement(vf.createIRI(exp + "test12"), vf.createIRI(exp + "id"), vf.createLiteral("hello 12"));
			conn.addStatement(vf.createIRI(exp + "test20"), vf.createIRI(exp + "p"), vf.createLiteral(20));
			conn.addStatement(vf.createIRI(exp + "test20"), vf.createIRI(exp + "id"), vf.createLiteral("hello 20"));
			conn.addStatement(vf.createIRI(exp + "test30"), vf.createIRI(exp + "p"), vf.createLiteral(30));
			conn.addStatement(vf.createIRI(exp + "test30"), vf.createIRI(exp + "id"), vf.createLiteral("hello 30"));
			conn.addStatement(vf.createIRI(exp + "test-12"), vf.createIRI(exp + "p"), vf.createLiteral(-12));
			conn.addStatement(vf.createIRI(exp + "test-12"), vf.createIRI(exp + "id"), vf.createLiteral("hello -12"));
			conn.commit();
		}
		ms.setEvaluationStrategyFactory(new DefaultEvaluationStrategyFactory(ms.getFederatedServiceResolver()) {
			@Override
			public EvaluationStrategy createEvaluationStrategy(Dataset dataset, TripleSource tripleSource, EvaluationStatistics evaluationStatistics) {
				DefaultEvaluationStrategy strategy = new DefaultEvaluationStrategy(tripleSource, dataset, getFederatedServiceResolver(), this.getQuerySolutionCacheThreshold(), evaluationStatistics, this.isTrackResultSize()) {

					@Override
					public QueryValueEvaluationStep precompile(ValueExpr expr, QueryEvaluationContext context) throws QueryEvaluationException {
						if (expr instanceof HDTCompareOp hcop) {
							boolean strict = QueryEvaluationMode.STRICT == getQueryEvaluationMode();
							return supplyBinaryValueEvaluation(hcop, (Value leftVal, Value rightVal) -> {
								System.out.println("compare: " + leftVal + "(" + leftVal.getClass().getSimpleName() + ")" + "/" + rightVal + "(" + rightVal.getClass().getSimpleName() + ")" );
								return BooleanLiteral
										.valueOf(QueryEvaluationUtil.compare(leftVal, rightVal, hcop.getOp(), strict));
							}, context);
						}
						return super.precompile(expr, context);
					}

				};
				Optional<QueryOptimizerPipeline> pipeline = this.getOptimizerPipeline();
				Objects.requireNonNull(strategy);
				pipeline.ifPresent(strategy::setOptimizerPipeline);
				strategy.setCollectionFactory(ms.getCollectionFactory());
				return strategy;
			}
		});
		SailRepository repo = new SailRepository(new NotifyingSailWrapper(ms) {
			@Override
			public NotifyingSailConnection getConnection() throws SailException {
				return new NotifyingSailConnectionWrapper(super.getConnection()) {
					@Override
					public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {

						tupleExpr = tupleExpr.clone();
						TestModelVisitor visitor = new TestModelVisitor();

						// https://github.com/eclipse-rdf4j/rdf4j/discussions/5085#discussioncomment-10140747
						QueryRoot root = tupleExpr instanceof QueryRoot qr ? qr : new QueryRoot(tupleExpr);
						System.out.println(root);
						System.out.println();
						visitor.meet(root);
						System.out.println(root);
						System.out.println();

						return super.evaluate(tupleExpr, dataset, bindings, includeInferred);
					}
				};
			}
		});
		Repositories.consume(repo, conn -> {
			SailTupleQuery query = (SailTupleQuery)conn.prepareTupleQuery("""
                    PREFIX ex: <http://example.org/#>
					SELECT ?id {
						?s ex:p ?o.
						?s ex:id ?id .
						FILTER (?o > 14)
					}
					""");

			query.evaluate(new SPARQLResultsCSVWriter(System.out));


		});
	}

	private static class TestModelVisitor extends AbstractQueryModelVisitor<RuntimeException> {
		@Override
		public void meet(Compare node) {
			node.replaceWith(new HDTCompareOp(node));
			super.meet(node);
		}
	}

	private static class HDTCompareOp extends BinaryValueOperator {
		private final Compare.CompareOp op;

		private HDTCompareOp(Compare base) {
			super(base.getLeftArg().clone(), base.getRightArg().clone());
			this.op = base.getOperator();
		}

		@Override
		public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
			visitor.meetOther(this);
		}

		public Compare.CompareOp getOp() {
			return op;
		}
	}

	@Test
	public void dumpCfg() throws IOException {
		String path = "";
		HDTOptions specs = HDTOptions.of();
		specs.setOptions(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);

		// generating 2 indexes, important for breath search
		specs.setOptions(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "SPO");
		// using cat to merge bigger dumps
		specs.setOptions("loader.type=cat;");
		// use k-way merge cat
		specs.setOptions("loader.cattree.kcat=20;");
		// use disk implementation of HDT
		specs.setOptions("loader.cattree.loadertype=disk;");
		// NFS BUG: do not delete cat directory at the end
		// specs.setOptions("hdtcat.deleteLocation=false");
		// directory with the HDT sub chunks
		Path tmp_cat_tree = new File(path + "tmp_cat_tree/").toPath();
		specs.setOptions("loader.cattree.location="+tmp_cat_tree.toAbsolutePath()+";");
		// directory with the recursive hdt cats
		Path tmp_hdt = new File(path + "tmp_hdt/").toPath();
		specs.setOptions("hdtcat.location.future="+tmp_hdt.toAbsolutePath()+";");
		// directory where HDT cat is running
		Path tmp_cat = new File(path + "tmp_cat/").toPath();
		specs.setOptions("hdtcat.location="+tmp_cat.toAbsolutePath()+";");
		// directory where HDT gen disk is running
		Path tmp_gen_disk = new File(path + "tmp_gen_disk/").toPath();
		specs.setOptions("loader.disk.location="+tmp_gen_disk.toAbsolutePath()+";");
		specs.setOptions(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, tmp_gen_disk.toAbsolutePath()+";");
		//specify the end location of the HDT file
		Path p = new File(path + "pageRankRDF.hdt").toPath();
		specs.setOptions("loader.cattree.futureHDTLocation="+p.toAbsolutePath()+";");
		// generated co-index on disk
		specs.setOptions("bitmaptriples.indexmethod=disk;");
		// use disk sequences instead of in-memory
		specs.setOptions(HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK, true);
		// directory where the bitmaps for the co-index is stored
		specs.setOptions(HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_LOCATION,tmp_gen_disk.toAbsolutePath());
		// can be escaped if there is enough memory
		specs.setOptions(HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_SUBINDEX,true);
		PrintWriter pw = new PrintWriter(System.out);
		specs.write(pw, false);
		pw.flush();
		/*
bitmaptriples.index.others=SPO
bitmaptriples.indexmethod=disk
bitmaptriples.sequence.disk=true
bitmaptriples.sequence.disk.location=tmp_gen_disk
bitmaptriples.sequence.disk.subindex=true
dictionary.type=dictionaryMultiObjLang
hdtcat.location=tmp_cat
hdtcat.location.future=tmp_hdt
loader.cattree.futureHDTLocation=pageRankRDF.hdt
loader.cattree.kcat=20
loader.cattree.loadertype=disk
loader.cattree.location=tmp_cat_tree
loader.disk.location=tmp_gen_disk
loader.type=cat
		 */
	}

	private static class HeadInputStream extends InputStream {
		private final byte[] headBuffer;
		private int headBufferLocation;
		private long read;
		private final InputStream is;

		private HeadInputStream(InputStream is, int size) {
			this.is = is;
			headBuffer = new byte[size];
		}

		@Override
		public int read() throws IOException {
			int r = is.read();
			if (r >= 0) {
				headBuffer[headBufferLocation] = (byte)r;
				headBufferLocation = (headBufferLocation + 1) % headBuffer.length;
				read++;
			}
			return r;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int r = is.read(b, off, len);

			if (r > 0) {
				if (r >= headBuffer.length) {
					System.arraycopy(b, off + r - headBuffer.length, headBuffer, 0, headBuffer.length);
					headBufferLocation = 0;
				} else {
					if (headBufferLocation + r > headBuffer.length) {
						// we need to write 2 chunks

						int bp = headBuffer.length - headBufferLocation;
						System.arraycopy(b, off, headBuffer, headBufferLocation, bp);
						System.arraycopy(b, off + bp, headBuffer, 0, r - bp);
						headBufferLocation = r - bp;
					} else {
						// full write
						System.arraycopy(b, off, headBuffer, headBufferLocation, r);
						headBufferLocation = (headBufferLocation + r) % headBuffer.length;
					}
				}
				read += r;
			}

			return r;
		}
		@Override
		public void close() throws IOException {
			is.close();
		}

		public byte[] getHeader() {
			if (read <= headBuffer.length) {
				return Arrays.copyOf(headBuffer, (int)read); // not enough to care
			}
			if (headBufferLocation == 0) {
				return Arrays.copyOf(headBuffer, headBuffer.length); // only one full copy
			}
			// double copy
			byte[] nb = new byte[headBuffer.length];
			System.arraycopy(headBuffer, headBufferLocation, nb, 0, headBuffer.length - headBufferLocation);
			System.arraycopy(headBuffer, 0, nb, headBuffer.length - headBufferLocation, headBufferLocation);
			return nb;
		}
	}

	private String mergePrefixes(String query) {
		StringBuilder b = new StringBuilder();
		String lastPrefix = null;
		Set<Namespace> namespaces = new HashSet<>();
		for (QueryPrologLexer.Token token : QueryPrologLexer.lex(query)) {
			switch (token.getType()) {
				case PREFIX_KEYWORD -> {} // nothing to do
				case PREFIX -> lastPrefix = token.getStringValue();
				case IRI -> {
					if (lastPrefix != null) {
						namespaces.add(new SimpleNamespace(lastPrefix, token.getStringValue()));
					} else {
						b.append(token.getStringValue());
					}
				}
				case LBRACKET -> {
					if (lastPrefix == null) {
						b.append(token.getStringValue());
					}
				}
				case RBRACKET -> {
					if (lastPrefix == null) {
						b.append(token.getStringValue()).append(' ');
					} else {
						lastPrefix = null;
					}

				}
				case COMMENT -> b.append(token.getStringValue()).append(' ');
				default -> b.append(token.getStringValue());
			}
		}
		return SPARQLQueries.getPrefixClauses(namespaces) + b;
	}

	@Test
	public void headerTest() throws IOException {
		byte[] bytes = """
				Lorem ipsum odor amet, consectetuer adipiscing elit. Integer justo ornare fames fermentum magnis nostra. Cursus phasellus hendrerit porta non molestie. Fusce blandit mauris lacus efficitur ac vehicula integer. Proin sodales duis semper accumsan scelerisque. Elit primis vivamus amet quisque porttitor enim luctus egestas at. Nam ex primis natoque, himenaeos quis est fermentum quam.
				    
				Penatibus sodales leo nisi cubilia dui; praesent aenean bibendum. Enim donec arcu vehicula amet netus. Dictum hendrerit maximus vehicula cursus interdum auctor hendrerit. Dui ex ultrices sit; in vehicula congue purus. Vitae sapien quam proin nascetur venenatis quisque nisl faucibus. Ornare nullam scelerisque ornare sapien lobortis auctor hendrerit. Semper leo quis nibh volutpat praesent pretium curabitur.
				    
				Facilisi nisl taciti cras, praesent per mauris vitae ultrices. Purus mattis eget in euismod laoreet congue sociosqu dui ad. Vel tellus luctus himenaeos enim vehicula tellus quis risus. Lobortis curae viverra convallis sodales class accumsan himenaeos sem. Cursus integer lacus; cursus habitasse nunc vitae. Accumsan nec efficitur; integer curabitur pretium cursus porta. Volutpat praesent egestas eleifend diam eget eu vitae.
				    
				Proin fames pretium congue orci cras odio condimentum. Senectus quam ornare justo condimentum sapien proin gravida. Velit suspendisse dignissim quam arcu urna. Ex diam orci vestibulum venenatis fames diam ex in. Purus eu imperdiet pretium cras nec nunc nunc mauris. Montes conubia nostra consectetur taciti quisque odio tempor ante nam. Tristique dui quis nascetur sollicitudin magna massa sed lorem efficitur. Aliquam auctor nisl sodales commodo litora lectus lectus platea. Vivamus suscipit per; fermentum vel integer donec.
				    
				Quis nullam duis lacinia pulvinar rutrum; risus quisque tristique. Aaliquet efficitur primis felis senectus primis. Auctor inceptos purus dui fusce aenean at sociosqu massa ipsum. Euismod vulputate lorem venenatis odio ligula. Ultrices fames aenean sapien ac euismod tincidunt maximus semper. Facilisis egestas eros netus interdum integer; gravida cubilia non. Vehicula purus euismod sapien senectus suspendisse. Ridiculus euismod justo conubia elementum mauris vestibulum suspendisse quisque. Faucibus ipsum hendrerit amet amet lectus class pretium.
				""".getBytes(StandardCharsets.UTF_8);
		ByteArrayInputStream is = new ByteArrayInputStream(bytes);

		HeadInputStream his = new HeadInputStream(is, 128);

		{
			String s1 = new String(his.readNBytes(64));
			String s2 = new String(his.getHeader());
			assertEquals(s1, s2);
		}
		{
			String s1 = new String(his.readNBytes(128));
			String s2 = new String(his.getHeader());
			assertEquals(s1, s2);
			String s3 = s2.substring(64) + new String(his.readNBytes(24))+ new String(his.readNBytes(40));
			String s4 = new String(his.getHeader());
			assertEquals(s3, s4);
		}
		{
			String s1 = new String(his.readNBytes(128));
			String s2 = new String(his.getHeader());
			assertEquals(s1, s2);
		}
	}

	@Test
	public void cleanupBadPrefixes() {

		System.out.println(mergePrefixes("""
			# test comment
			PREFIX ex: <http://example.org/#>
			PREFIX ex: <http://example.org/#>
			PREFIX ex: <http://example.org/#>
			
			BASE  <http://example2.org/#>
							
			SELECT * {?s ?p ?o }
			"""));
		System.out.println("***************");
		System.out.println(mergePrefixes("""
			# test comment
			PREFIX ex: <http://example.org/#>
			PREFIX ex: <http://example2.org/#>
			PREFIX ex: <http://example3.org/#>
			
			BASE  <http://example2.org/#>
							
			SELECT * {?s ?p ?o }
			"""));

		//List<Namespace> namespaces = defaultPrefixes.entrySet().stream()
		//		.filter(e -> !prefixes.contains(e.getKey())).map(Map.Entry::getValue)
		//		.collect(Collectors.toList());
//
		//return SPARQLQueries.getPrefixClauses(namespaces) + " " + sparqlQuery;

	}
}

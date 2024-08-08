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
import com.the_qa_company.qendpoint.utils.rdf.ClosableResult;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.transaction.QueryEvaluationMode;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
}

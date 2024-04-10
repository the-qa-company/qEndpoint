package com.the_qa_company.qendpoint.store.hand;

import com.the_qa_company.qendpoint.compiler.CompiledSail;
import com.the_qa_company.qendpoint.compiler.SparqlRepository;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.utils.rdf.ClosableResult;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;

@Disabled
public class HandTest {
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

}

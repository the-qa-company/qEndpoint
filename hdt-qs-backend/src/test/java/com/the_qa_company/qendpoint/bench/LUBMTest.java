package com.the_qa_company.qendpoint.bench;

import com.the_qa_company.qendpoint.compiler.CompiledSail;
import com.the_qa_company.qendpoint.compiler.SparqlRepository;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.search.HDTQuery;
import com.the_qa_company.qendpoint.core.search.HDTQueryResult;
import com.the_qa_company.qendpoint.core.search.HDTQueryTool;
import com.the_qa_company.qendpoint.core.search.HDTQueryToolFactory;
import com.the_qa_company.qendpoint.core.search.component.HDTConstant;
import com.the_qa_company.qendpoint.core.search.exception.HDTSearchTimeoutException;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.utils.rdf.ClosableResult;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

@Ignore("hand tests")
public class LUBMTest {

	@Test
	public void lubmQEPTest() throws IOException {
		StopWatch watch = new StopWatch();
		Path path = Path.of("C:\\Users\\wilat\\workspace\\lubm\\lubm_store");
		SparqlRepository repo = CompiledSail.compiler()
				.withEndpointFiles(
						new EndpointFiles(path.resolve("native-store"), path.resolve("hdt-store"), "lubm12k.hdt"))
				.compileToSparqlRepository();
		try {
			String[] queries = {
					// L1
					String.join("\n", "PREFIX ub: <https://wdaqua.eu/lubm#>", "SELECT ?x ?y ?z WHERE {",
							"  ?z ub:subOrganizationOf ?y.", "  ?y rdf:type ub:University.",
							"  ?z rdf:type ub:Department.", "  ?x ub:memberOf ?z.", "  ?x rdf:type ub:GraduateStudent.",
							"  ?x ub:undergraduateDegreeFrom ?y.", "}"),
					// L2
					String.join("\n", "PREFIX ub: <https://wdaqua.eu/lubm#>", "SELECT ?x WHERE {",
							"    ?x rdf:type ub:Course .", "    ?x ub:name ?y .", "}"),
					// L3
					String.join("\n", "PREFIX ub: <https://wdaqua.eu/lubm#>", "SELECT ?x ?y ?z WHERE {",
							"  ?x rdf:type ub:Undergraduate-Student.", "  ?y rdf:type ub:University.",
							"  ?z rdf:type ub:Department.", "  ?x ub:memberOf ?z.", "  ?z ub:subOrganizationOf ?y.",
							"  ?x ub:under-graduateDegreeFrom ?y.", "}"),
					// L4
					String.join("\n", "PREFIX ub: <https://wdaqua.eu/lubm#> ", "SELECT ?x WHERE {",
							"  ?x ub:worksFor <http://www.Department0.University0.edu>.",
							"  ?x rdf:type ub:FullProfessor.", "  ?x ub:name ?y1.", "  ?x ub:emailAddress ?y2.",
							"  ?x ub:telephone ?y3.", "}"),
					// L5
					String.join("\n", "PREFIX ub: <https://wdaqua.eu/lubm#>", "SELECT ?x WHERE {",
							"  ?x ub:subOrganizationOf <http://www.Department0.University0.edu>.",
							"  ?x rdf:type ub:Research-Group.", "}"),
					// L6
					String.join("\n", "PREFIX ub: <https://wdaqua.eu/lubm#>", "SELECT ?x ?y WHERE { ",
							"    ?y ub:subOrganizationOf <http://www.University0.edu> . ",
							"    ?y rdf:type ub:Department . ",
							"    ?x ub:worksFor ?y . ?x rdf:type ub:FullProfessor . ", "}"),
					// L7
					String.join("\n", "PREFIX ub: <https://wdaqua.eu/lubm#>", "SELECT ?x ?y ?z WHERE { ",
							"    ?y ub:teacherOf ?z . ", "    ?y rdf:type ub:FullProfessor . ",
							"    ?z rdf:type ub:Course . ", "    ?x ub:advisor ?y .", "}") };

			System.out.println("Store loaded in " + watch.stopAndShow());

			long[] times = new long[queries.length];
			for (int i = 5; i < 6; i++) {
				String query = queries[i];
				watch.reset();
				try (ClosableResult<TupleQueryResult> tr = repo.executeTupleQuery(query, 3_600)) {
					TupleQueryResult result = tr.getResult();
					result.stream().flatMap(set -> set.getBindingNames().stream().map(set::getBinding)).forEach(b -> {
						Value v = b.getValue();
						if (v != null) {
							v.stringValue();
						}
					});
				} catch (Exception e) {
					e.printStackTrace();
				}
				watch.stop();
				times[i] = watch.getMeasureMillis();
			}
			for (int i = 0; i < times.length; i++) {
				System.out.println("query #" + (i + 1) + " time: " + times[i] + "ms");
			}

		} finally {
			repo.shutDown();
		}
	}

	@Test
	public void lubmTest() throws IOException {
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK, true,
				HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_LOCATION,
				Path.of("N:\\qEndpoint\\qendpoint\\hdt-store\\indexWork"));
		StopWatch watch = new StopWatch();
		String path = "C:\\Users\\wilat\\workspace\\lubm\\lubm_store\\hdt-store\\lubm12k.hdt";
		try (HDT hdt = HDTManager.mapIndexedHDT(path, spec, ProgressListener.sout())) {
			System.out.println("HDT INDEXED WITH " + hdt.getTriples().getNumberOfElements() + " triples in "
					+ watch.stopAndShow());
			HDTQueryTool tool = HDTQueryToolFactory.createQueryTool(hdt);

			/*
			 * select ?p ?type { ?s wdt:P31 wd:Q5 . ?s ?p ?o . ?o wdt:P31 ?type
			 * . }
			 */

			// register the WD prefixes
			tool.registerPrefix("ub", "https://wdaqua.eu/lubm#");
			tool.registerPrefix("rdf", RDF.NAMESPACE);
			List<HDTQuery> queries = List.of(tool.createQuery(tool.triple("?z", "ub:subOrganizationOf", "?y"),
					tool.triple("?y", "rdf:type", "ub:University"), tool.triple("?z", "rdf:type", "ub:Department"),
					tool.triple("?x", "ub:memberOf", "?z"), tool.triple("?x", "rdf:type", "ub:GraduateStudent"),
					tool.triple("?x", "ub:undergraduateDegreeFrom", "?y")),
					tool.createQuery(tool.triple("?x", "rdf:type", "ub:Course"), tool.triple("?x", "ub:name", "?y")),
					tool.createQuery(tool.triple("?x", "rdf:type", "ub:Undergraduate-Student"),
							tool.triple("?y", "rdf:type", "ub:University"),
							tool.triple("?z", "rdf:type", "ub:Department"), tool.triple("?x", "ub:memberOf", "?z"),
							tool.triple("?z", "ub:subOrganizationOf", "?y"),
							tool.triple("?x", "ub:under-graduateDegreeFrom", "?y")),
					tool.createQuery(tool.triple("?x", "ub:worksFor", "<http://www.Department0.University0.edu>"),
							tool.triple("?x", "rdf:type", "ub:FullProfessor"), tool.triple("?x", "ub:name", "?y1"),
							tool.triple("?x", "ub:emailAddress", "?y2"), tool.triple("?x", "ub:telephone", "?y3")),
					tool.createQuery(
							tool.triple("?x", "ub:subOrganizationOf", "<http://www.Department0.University0.edu>"),
							tool.triple("?x", "rdf:type", "ub:Research-Group")),

					tool.createQuery(tool.triple("?y", "ub:subOrganizationOf", "<http//www.University0.edu>"),
							tool.triple("?y", "rdf:type", "ub:Department"), tool.triple("?x", "ub:worksFor", "?y"),
							tool.triple("?x", "rdf:type", "ub:FullProfessor")),

					tool.createQuery(tool.triple("?y", "ub:teacherOf", "?z"),
							tool.triple("?y", "rdf:type", "ub:FullProfessor"),
							tool.triple("?z", "rdf:type", "ub:Course"), tool.triple("?x", "ub:advisor", "?y")));
			String[][] projections = new String[][] { { "x", "y", "z" }, { "x" }, { "x", "y", "z" }, { "x" }, { "x" },
					{ "x", "y" }, { "x", "y", "z" }, };

			long[] times = new long[queries.size()];
			for (int i = 0; i < queries.size(); i++) {
				HDTQuery query = queries.get(i);
				String[] projection = projections[i];
				// 1h
				query.setTimeout(3_600_000L);

				System.out.println("---- query " + i + " ----");
				System.out.println(query);
				System.out.println("-----------------");

				watch.reset();
				int count = 0;
				try {

					Iterator<HDTQueryResult> it = query.query();

					while (it.hasNext()) {
						HDTQueryResult result = it.next();
						// System.out.print(count + " - [" + watch.stopAndShow()
						// + "] ");
						for (String varName : projection) {
							HDTConstant value = result.getComponent(varName);
							// force usage of the object value
							value.stringValue();
							// System.out.print(varName + "=" + value + " ");
						}
						// System.out.println();
						++count;
					}
				} catch (HDTSearchTimeoutException e) {
					e.printStackTrace();
				}
				watch.stop();
				times[i] = watch.getMeasureMillis();
				System.out.println("query #" + (i + 1) + " " + count + " in " + times[i] + " ms");
			}
			for (int i = 0; i < times.length; i++) {
				System.out.println("query #" + (i + 1) + " time: " + times[i] + "ms");
			}
		}
	}
}

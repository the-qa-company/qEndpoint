package com.the_qa_company.qendpoint.core.search.query;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.search.HDTQuery;
import com.the_qa_company.qendpoint.core.search.HDTQueryResult;
import com.the_qa_company.qendpoint.core.search.HDTQueryTool;
import com.the_qa_company.qendpoint.core.search.HDTQueryToolFactory;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import org.junit.Ignore;
import org.junit.Test;
import com.the_qa_company.qendpoint.core.search.component.HDTConstant;
import com.the_qa_company.qendpoint.core.search.exception.HDTSearchTimeoutException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NestedJoinQueryIteratorTest {
	@Test
	public void joinTest() throws ParserException, IOException {
		List<TripleString> triples = List.of(new TripleString("a", "p2", "\"aaa\""), new TripleString("a", "p", "d"),
				new TripleString("b", "p", "e"), new TripleString("b", "p2", "\"bbb\""),
				new TripleString("d", "p2", "\"ccc\""), new TripleString("e", "p2", "\"eee\""));
		try (HDT hdt = HDTManager.generateHDT(triples.iterator(), HDTTestUtils.BASE_URI, HDTOptions.of(),
				ProgressListener.ignore())) {
			HDTManager.indexedHDT(hdt, ProgressListener.ignore());

			HDTQueryTool tool = HDTQueryToolFactory.createQueryTool(hdt);

			HDTQuery query = tool.createQuery(tool.triple("<a>", "<p>", "?o"), tool.triple("?o", "<p2>", "?o2"));
			NestedJoinQueryIterator iterator = new NestedJoinQueryIterator(hdt, query, 0);
			assertTrue(iterator.hasNext());
			HDTQueryResult result = iterator.next();
			assertEquals("d", result.getComponent("o").stringValue());
			assertEquals("\"ccc\"", result.getComponent("o2").stringValue());
			assertFalse(iterator.hasNext());
		}
	}

	@Test(expected = HDTSearchTimeoutException.class)
	public void joinTimeoutTest() throws ParserException, IOException, InterruptedException {
		long waitTime = 300;
		List<TripleString> triples = List.of(new TripleString("a", "p2", "\"aaa\""), new TripleString("a", "p", "d"),
				new TripleString("a", "p", "f"), new TripleString("b", "p", "e"),
				new TripleString("b", "p2", "\"bbb\""), new TripleString("d", "p2", "\"ccc\""),
				new TripleString("f", "p2", "\"fff\""), new TripleString("e", "p2", "\"eee\""));
		try (HDT hdt = HDTManager.generateHDT(triples.iterator(), HDTTestUtils.BASE_URI, HDTOptions.of(),
				ProgressListener.ignore())) {
			HDTManager.indexedHDT(hdt, ProgressListener.ignore());

			HDTQueryTool tool = HDTQueryToolFactory.createQueryTool(hdt);

			HDTQuery query = tool.createQuery(tool.triple("<a>", "<p>", "?o"), tool.triple("?o", "<p2>", "?o2"));
			query.setTimeout(waitTime);

			NestedJoinQueryIterator iterator = new NestedJoinQueryIterator(hdt, query, query.getTimeout());
			assertTrue(iterator.hasNext());
			iterator.next(); // consume the next
			StopWatch sw = new StopWatch();
			sleepReal(waitTime * 2); // take out time
			System.out.println(sw.stopAndShow());
			assertTrue(iterator.hasNext()); // should cause the Timeout
		}
	}

	public static void sleepReal(long millis) throws InterruptedException {
		long sl = System.currentTimeMillis();
		long end = sl + millis;
		do {
			// noinspection BusyWait
			Thread.sleep(Math.max(1, end - sl));
		} while ((sl = System.currentTimeMillis()) < end);
	}

	@Ignore("hand test")
	public static class HandTest {
		@Test
		public void loadTest() throws IOException {
			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK, true,
					HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_LOCATION,
					Path.of("N:\\qEndpoint\\qendpoint\\hdt-store\\indexWork"));
			StopWatch watch = new StopWatch();
			String path = "C:\\Users\\wilat\\AppData\\Roaming\\qendpoint\\hdt-store\\index_dev.hdt";
			// String path = "N:\\qEndpoint\\qendpoint\\hdt-store\\wdb.hdt";
			try (HDT hdt = HDTManager.mapIndexedHDT(path, spec, ProgressListener.sout())) {
				System.out.println("HDT INDEXED WITH " + hdt.getTriples().getNumberOfElements() + " triples in "
						+ watch.stopAndShow());
				// (WD all dataset) HDT INDEXED WITH 17491304071 triples in 32
				// sec 396 ms 922 us
				// (WDBench dataset) HDT INDEXED WITH 1253567798 triples in 2
				// sec 384 ms 409 us
				HDTQueryTool tool = HDTQueryToolFactory.createQueryTool(hdt);

				/*
				 * select ?p ?type { ?s wdt:P31 wd:Q5 . ?s ?p ?o . ?o wdt:P31
				 * ?type . }
				 */

				// register the WD prefixes
				tool.registerPrefix("wd", "http://www.wikidata.org/entity/");
				tool.registerPrefix("wdt", "http://www.wikidata.org/prop/direct/");

				HDTQuery query = tool.createQuery(tool.triple("?s", "wdt:P31", "wd:Q5"), tool.triple("?s", "?p", "?o"),
						tool.triple("?o", "wdt:P31", "?type"));

				// 5min
				query.setTimeout(300_000_000L);

				System.out.println("----- query -----");
				System.out.println(query);
				System.out.println("-----------------");
				/*
				 * ----- query ----- SELECT ?p ?s ?type ?o { ?s
				 * http://www.wikidata.org/prop/direct/P31
				 * http://www.wikidata.org/entity/Q5 . ?s ?p ?o . ?o
				 * http://www.wikidata.org/prop/direct/P31 ?type . }
				 * -----------------
				 */

				watch.reset();
				int count = 0;
				try {

					Iterator<HDTQueryResult> it = query.query();

					while (it.hasNext()) {
						HDTQueryResult result = it.next();
						HDTConstant pValue = result.getComponent("p");
						HDTConstant typeValue = result.getComponent("type");

						++count;
						System.out.println(
								count + " - [" + watch.stopAndShow() + "] p=" + pValue + " / type=" + typeValue);
						if (count == 100) {
							break;
						}
					}
				} catch (HDTSearchTimeoutException e) {
					e.printStackTrace();
				}
				System.out.println(count + " in " + watch.stopAndShow());
				// with stop after 1,000 results (without printing)
				// 1000 in 206 ms 850 us (without timeout method)
				// 1000 in 210 ms 241 us (with timeout method)

				// with stop after 1,000,000 results (without printing)
				// 1000000 in 47 sec 940 ms 871 us (with timeout method)

				// without any stop using WDBench dataset (only direct property)
				// 112833152 in 2 min 34 sec 889 ms 946 us
			}
		}

		@Test
		public void test() {

		}
	}
}

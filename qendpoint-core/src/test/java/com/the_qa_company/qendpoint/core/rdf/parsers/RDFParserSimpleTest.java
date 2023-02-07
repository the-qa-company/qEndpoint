package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import org.junit.Assert;
import org.junit.Test;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.util.Iterator;

public class RDFParserSimpleTest extends AbstractNTriplesParserTest {
	@Override
	protected RDFParserCallback createParser() {
		return new RDFParserSimple();
	}

	@Test
	public void ingestTest() throws IOException, InterruptedException, ParserException {
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(1_000_001,
				42);
		LargeFakeDataSetStreamSupplier supplier2 = LargeFakeDataSetStreamSupplier
				.createSupplierWithMaxTriples(1_000_001, 42);

		PipedOutputStream out = new PipedOutputStream();
		PipedInputStream in = new PipedInputStream();
		in.connect(out);

		RuntimeException[] re = new RuntimeException[1];
		Thread t = new Thread(() -> {
			try {
				Iterator<TripleString> it = supplier.createTripleStringStream();
				PrintStream ps = new PrintStream(out);
				while (it.hasNext()) {
					TripleString next = it.next();
					next.dumpNtriple(ps);
					ps.flush();
				}
				out.close();
			} catch (RuntimeException tt) {
				re[0] = tt;
			} catch (Throwable tt) {
				re[0] = new RuntimeException(tt);
			}
		});
		t.start();

		RDFParserCallback parser = createParser();

		Iterator<TripleString> it = supplier2.createTripleStringStream();

		int[] count = new int[1];
		StopWatch watch = new StopWatch();
		watch.reset();
		parser.doParse(in, "http://example.org/#", RDFNotation.NTRIPLES, true, (triple, pos) -> {
			Assert.assertTrue(it.hasNext());
			Assert.assertEquals(it.next(), triple);
			if (count[0] % 100_000 == 0) {
				System.out.println(count[0] + " triples " + watch.stopAndShow());
			}
			count[0]++;
		});

		t.join();
		if (re[0] != null) {
			throw re[0];
		}
	}
}

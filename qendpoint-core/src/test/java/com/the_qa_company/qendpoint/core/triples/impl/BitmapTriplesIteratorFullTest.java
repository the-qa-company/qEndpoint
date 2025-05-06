package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BitmapTriplesIteratorFullTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void loadTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();
		Path test = root.resolve("test.hdt");
		LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(34567, 45).withMaxElementSplit(30)
				.withMaxLiteralSize(10).createAndSaveFakeHDT(HDTOptions.empty(), test);

		try (HDT hdt = HDTManager.mapHDT(test)) {
			BitmapTriples triples = (BitmapTriples) hdt.getTriples();
			BitmapTriplesIterator itex = new BitmapTriplesIterator(triples, new TripleID());
			BitmapTriplesIteratorFull itac = new BitmapTriplesIteratorFull(triples, triples.getNumberOfElements(),
					triples.getOrder());

			long idx = 0;
			while (itex.hasNext()) {
				assertTrue(itac.hasNext());
				TripleID ex = itex.next();
				TripleID ac = itac.next();

				assertEquals("invalid at idx #" + idx++, ex, ac);
			}
			if (itac.hasNext()) {
				fail("More elements #" + idx + "=" + itac.next());
			}
		}

	}

	@Test
	@Ignore
	public void timeTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();
		Path test = root.resolve("test.hdt");
		LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(34567030, 45).withMaxElementSplit(30)
				.withMaxLiteralSize(10).createAndSaveFakeHDT(HDTOptions.empty(), test);

		try (HDT hdt = HDTManager.mapHDT(test)) {
			BitmapTriples triples = (BitmapTriples) hdt.getTriples();
			StopWatch sw = new StopWatch();

			for (int i = 0; i < 10; i++) {
				sw.reset();
				BitmapTriplesIteratorFull itac = new BitmapTriplesIteratorFull(triples, triples.getNumberOfElements(),
						triples.getOrder());
				while (itac.hasNext()) {
					itac.next();
				}
				System.out.println("ac#" + i + " / " + sw.stopAndShow());
			}
			for (int i = 0; i < 10; i++) {
				sw.reset();
				BitmapTriplesIterator itex = new BitmapTriplesIterator(triples, new TripleID());
				while (itex.hasNext()) {
					itex.next();
				}
				System.out.println("ex#" + i + " / " + sw.stopAndShow());
			}
			for (int i = 0; i < 10; i++) {
				sw.reset();
				BitmapTriplesIteratorFull itac = new BitmapTriplesIteratorFull(triples, triples.getNumberOfElements(),
						triples.getOrder());
				while (itac.hasNext()) {
					itac.next();
				}
				System.out.println("ac#" + i + " / " + sw.stopAndShow());
			}
			for (int i = 0; i < 10; i++) {
				sw.reset();
				BitmapTriplesIterator itex = new BitmapTriplesIterator(triples, new TripleID());
				while (itex.hasNext()) {
					itex.next();
				}
				System.out.println("ex#" + i + " / " + sw.stopAndShow());
			}
		}

	}

}

package com.the_qa_company.qendpoint.core.triples.impl;

import java.io.IOException;
import java.nio.file.Path;

import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import org.apache.commons.io.file.PathUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class BitmapTriplesIteratorTest {

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() throws IOException {
//		HDT hdt = HDTManager.mapHDT("/Users/mck/hdt/swdf.hdt", null);
//
//		int t = (int) hdt.getTriples().getNumberOfElements();
//		BitmapTriplesIterator it = new BitmapTriplesIterator((BitmapTriples) hdt.getTriples(), t-10, t);
//
//		while(it.hasNext()) {
//			System.out.println(it.next());
//		}
	}

	@Test
	public void jumpTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();

		try {
			LargeFakeDataSetStreamSupplier sup = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(1000, 32);
			Path hdtPath = root.resolve("test.hdt");
			sup.createAndSaveFakeHDT(HDTOptions.empty(), hdtPath);

			try (HDT hdt = HDTManager.mapIndexedHDT(hdtPath)) {
				IteratorTripleID it = hdt.getTriples().searchAll();

				assertTrue("bad class: " + it.getClass(), it instanceof BitmapTriplesIterator);

				TripleID start = it.next().clone();


				for (int i = 0; i < 458; i++) {
					assertTrue(it.hasNext());
					it.next();
				}


				TripleID lastTest = it.next().clone();

				assertNotEquals(start, lastTest);

				long posLast = it.getLastTriplePosition();

				it.goToStart();
				assertEquals(start, it.next());

				assertEquals(0, it.getLastTriplePosition());

				it.goTo(posLast);

				assertEquals(lastTest, it.next());

				assertEquals(posLast, it.getLastTriplePosition());
			}
		} finally {
			PathUtils.deleteDirectory(root);
		}


	}

}

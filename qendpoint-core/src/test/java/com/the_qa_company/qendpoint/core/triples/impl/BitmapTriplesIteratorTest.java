package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import org.apache.commons.io.file.PathUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BitmapTriplesIteratorTest {

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Before
	public void setUp() throws Exception {
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

	private static final String JUMP_XYZ_DATASET = """
			@prefix ex: <http://example.org/#> .

			ex:s1   ex:p1 ex:o0000, ex:o0001, ex:o0002, ex:o0003, ex:o0004, ex:o0005 ;
			        ex:p2 ex:o0000, ex:o0002, ex:o0003, ex:o0004, ex:o0005 ;
			        ex:p3 ex:o0000, ex:o0001, ex:o0002, ex:o0003, ex:o0004, ex:o0005 ;
			        ex:p4 ex:o0000, ex:o0001, ex:o0002, ex:o0004, ex:o0005 ;
			        ex:p5 ex:o0000, ex:o0001, ex:o0002, ex:o0003, ex:o0004, ex:o0005 .


			ex:s2   ex:p1 ex:o0006, ex:o0007, ex:o0008, ex:o0009, ex:o0010, ex:o0011 ;
			        ex:p2 ex:o0008, ex:o0009, ex:o0010, ex:o0011 ;
			        ex:p3 ex:o0007, ex:o0008, ex:o0009, ex:o0010, ex:o0011 ;
			        ex:p4 ex:o0006, ex:o0007, ex:o0008, ex:o0009, ex:o0010, ex:o0011 .


			ex:s3   ex:p1 ex:o0003, ex:o0005, ex:o0007, ex:o0009, ex:o0011, ex:o0015 ;
			        ex:p2 ex:o0003, ex:o0005, ex:o0007, ex:o0009, ex:o0011, ex:o0015 ;
			        ex:p3 ex:o0003, ex:o0005, ex:o0007, ex:o0009, ex:o0011, ex:o0015 ;
			        ex:p4 ex:o0003, ex:o0005, ex:o0007 ;
			        ex:p5 ex:o0003, ex:o0005, ex:o0007, ex:o0009, ex:o0011, ex:o0015 ;
			        ex:p6 ex:o0003, ex:o0007, ex:o0009, ex:o0011, ex:o0015 ;
			        ex:p7 ex:o0003, ex:o0005, ex:o0009, ex:o0011, ex:o0015 .


			ex:s4   ex:p1 ex:o0003, ex:o0005, ex:o0007, ex:o0009, ex:o0011, ex:o0015 ;
			        ex:p2 ex:o0005, ex:o0007, ex:o0009, ex:o0011, ex:o0015 ;
			        ex:p3 ex:o0003, ex:o0005, ex:o0009, ex:o0011, ex:o0015 ;
			        ex:p4 ex:o0003, ex:o0005, ex:o0007, ex:o0009, ex:o0011, ex:o0015 ;
			        ex:p5 ex:o0003, ex:o0005, ex:o0007, ex:o0009 .

			""";
	private static final long JUMP_XYZ_DATASET_X = 4;
	private static final long JUMP_XYZ_DATASET_Y = 4;

	@Test
	public void jumpXTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();

		try {

			Path hdtPath = root.resolve("test.hdt");

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "spo,sop,pos,pso,ops,osp",
					HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true);

			try (HDT hdt = HDTManager.generateHDT(
					new ByteArrayInputStream(JUMP_XYZ_DATASET.getBytes(StandardCharsets.UTF_8)),
					LargeFakeDataSetStreamSupplier.BASE_URI, RDFNotation.TURTLE, spec, ProgressListener.ignore())) {
				hdt.saveToHDT(hdtPath);
			}

			try (HDT hdt = HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore())) {

				IteratorTripleID ittt = hdt.getTriples().searchAll();
				assertTrue("bad class: " + ittt.getClass(), ittt instanceof BitmapTriplesIterator);

				for (int sid = 1; sid <= JUMP_XYZ_DATASET_X; sid++) {
					IteratorTripleID it = hdt.getTriples().searchAll();
					IteratorTripleID itex = hdt.getTriples().searchAll();

					assertTrue(it.gotoSubject(sid));

					assertEquals(sid, it.next().getSubject());

					long s;
					do {
						assertTrue(itex.hasNext());
						s = itex.next().getSubject();
					} while (s < sid);
					assertEquals(sid, s);

					assertTrue(it.hasNext());
					do {
						TripleID ac = it.next();
						assertTrue(itex.hasNext());
						TripleID ex = itex.next();
						assertEquals(itex.getLastTriplePosition(), it.getLastTriplePosition());

						assertEquals(ex, ac);
					} while (it.hasNext());

					assertFalse(itex.hasNext());
				}
			}
		} finally {
			PathUtils.deleteDirectory(root);
		}
	}

	@Test
	public void jumpYTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();

		try {

			Path hdtPath = root.resolve("test.hdt");

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "spo,sop,pos,pso,ops,osp",
					HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true);

			try (HDT hdt = HDTManager.generateHDT(
					new ByteArrayInputStream(JUMP_XYZ_DATASET.getBytes(StandardCharsets.UTF_8)),
					LargeFakeDataSetStreamSupplier.BASE_URI, RDFNotation.TURTLE, spec, ProgressListener.ignore())) {
				hdt.saveToHDT(hdtPath);
			}

			try (HDT hdt = HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore())) {

				IteratorTripleID ittt = hdt.getTriples().searchAll();
				assertTrue("bad class: " + ittt.getClass(), ittt instanceof BitmapTriplesIterator);

				String lastPosData;

				for (int sid = 1; sid <= JUMP_XYZ_DATASET_X; sid++) {
					for (int pid = 1; pid <= JUMP_XYZ_DATASET_Y; pid++) {
						IteratorTripleID it = hdt.getTriples().searchAll();
						IteratorTripleID itex = hdt.getTriples().searchAll();

						assertTrue(it.gotoSubject(sid));
						assertTrue(it.gotoPredicate(pid));

						TripleID next = it.next();
						lastPosData = "[sid:" + sid + "/pid:" + pid + "][ac:" + it.getLastTriplePosition() + "/ex"
								+ itex.getLastTriplePosition() + "]" + next;
						assertEquals("invalid pos: " + lastPosData, sid, next.getSubject());
						assertEquals("invalid pos: " + lastPosData, pid, next.getPredicate());

						long s;
						long p;
						do {
							assertTrue(itex.hasNext());
							TripleID next1 = itex.next();
							s = next1.getSubject();
							p = next1.getPredicate();
							lastPosData = "[sid:" + sid + "/pid:" + pid + "][ac:" + it.getLastTriplePosition() + "/ex"
									+ itex.getLastTriplePosition() + "]" + next1;
						} while (s < sid || p < pid);
						assertEquals(lastPosData, sid, s);
						assertEquals(lastPosData, pid, p);

						assertTrue(it.hasNext());
						do {
							TripleID ac = it.next();
							assertTrue(itex.hasNext());
							TripleID ex = itex.next();
							lastPosData = "[sid:" + sid + "/pid:" + pid + "][ac:" + it.getLastTriplePosition() + "/ex"
									+ itex.getLastTriplePosition() + "]" + ac + "/" + ex;
							assertEquals(lastPosData, itex.getLastTriplePosition(), it.getLastTriplePosition());

							assertEquals(lastPosData, ex, ac);
						} while (it.hasNext());

						assertFalse(itex.hasNext());
					}
				}
			}
		} finally {
			PathUtils.deleteDirectory(root);
		}

	}

	@Test
	public void jumpXYZTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();

		try {
			Path hdtPath = root.resolve("test.hdt");

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "spo,sop,pos,pso,ops,osp",
					HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true);
			final int count = 100_000;
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(count, 567890987).withMaxElementSplit(50).withMaxLiteralSize(20);

			supplier.createAndSaveFakeHDT(spec, hdtPath);

			Random rnd = new Random(34567);

			try (HDT hdt = HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore())) {
				int elements = (int) hdt.getTriples().getNumberOfElements();

				for (int i = 0; i < count; i++) {
					int idx = rnd.nextInt(elements);

					IteratorTripleID it = hdt.getTriples().searchAll();

					assertTrue(it.canGoTo());

					it.goTo(idx);

					TripleID current = it.next().clone();
					assertEquals(idx, it.getLastTriplePosition());

					for (int member = 0; member < 3; member++) {
						IteratorTripleID itac = hdt.getTriples().searchAll(TripleComponentOrder.SPO.mask);
						assertSame("invalid order (" + member + "/" + i + ")", itac.getOrder(),
								TripleComponentOrder.SPO);

						// test subject
						assertTrue("Can't jump to subject " + current + " (" + member + "/" + i + ")",
								itac.canGoToSubject() && itac.gotoSubject(current.getSubject()));

						if (member >= 1) {
							// test predicate
							assertTrue("Can't jump to predicate " + current + " (" + member + "/" + i + ")",
									itac.canGoToPredicate() && itac.gotoPredicate(current.getPredicate()));

							if (member >= 2) {
								// test object
								assertTrue("Can't jump to object " + current + " (" + member + "/" + i + ")",
										itac.canGoToObject() && itac.gotoObject(current.getObject()));
							}
						}

						assertTrue("for " + current + " (" + member + "/" + i + ")", itac.hasNext());
						TripleID next = itac.next();
						String err = "invalid next " + next + " != " + current + " (" + member + "/" + i + ")";
						switch (member) {
						case 2: // object
							assertEquals("object err " + err, current.getObject(), next.getObject());
						case 1: // predicate
							assertEquals("predicate err " + err, current.getPredicate(), next.getPredicate());
						case 0: // subject only
							assertEquals("subject err " + err, current.getSubject(), next.getSubject());
							break;
						default:
							fail("bad member: " + member);
							break;
						}
						if (member == 2) {
							assertEquals("idx err " + err, idx, itac.getLastTriplePosition());
							TripleID newCurrent = itac.next();
							assertTrue("idx err " + err, idx < itac.getLastTriplePosition());

							if (current.getSubject() == newCurrent.getSubject()) {
								// no jump on X, we should have the sam
								assertTrue("Can't jump to subject " + current + " (" + member + "/" + i + ")",
										itac.gotoSubject(current.getSubject()));

								if (current.getPredicate() == newCurrent.getPredicate()) {
									// no jump on Y, we should have the same
									assertTrue("Can't jump to subject " + current + " (" + member + "/" + i + ")",
											itac.gotoPredicate(current.getPredicate()));

									assertFalse("Can't jump to subject " + current + " (" + member + "/" + i + ")",
											itac.gotoObject(current.getObject()));
								} else {
									assertFalse("Can't jump to subject " + current + " (" + member + "/" + i + ")",
											itac.gotoPredicate(current.getPredicate()));
								}

							} else {
								assertFalse("Can't jump to subject " + current + " (" + member + "/" + i + ")",
										itac.gotoSubject(current.getSubject()));
							}

						} else {
							assertTrue("idx err " + err, idx >= itac.getLastTriplePosition());
						}
					}
				}
			}
		} finally {
			PathUtils.deleteDirectory(root);
		}
	}
}

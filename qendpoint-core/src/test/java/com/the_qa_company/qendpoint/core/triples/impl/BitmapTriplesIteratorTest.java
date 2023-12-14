package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BitmapTriplesIteratorTest {

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

	public static TripleComponentRole findHighDelta(TripleID origin, TripleID jump) {
		if (jump.getSubject() != origin.getSubject()) {
			return TripleComponentRole.SUBJECT;
		}
		if (jump.getPredicate() != origin.getPredicate()) {
			return TripleComponentRole.PREDICATE;
		}

		return TripleComponentRole.OBJECT;
	}

	public static void assertStatesEqual(String message, IteratorTripleID it1, IteratorTripleID it2) {
		assertTrue(it1 instanceof BitmapTriplesIterator);
		assertTrue(it2 instanceof BitmapTriplesIterator);

		BitmapTriplesIterator btit1 = (BitmapTriplesIterator) it1;
		BitmapTriplesIterator btit2 = (BitmapTriplesIterator) it2;

		assertEquals(message + " -> x not equals", btit1.x, btit2.x);
		assertEquals(message + " -> y not equals", btit1.y, btit2.y);
		// assertEquals(message + " -> z not equals", btit1.z, btit2.z);

		assertEquals(message + " -> posY not equals", btit1.posY, btit2.posY);
		assertEquals(message + " -> posZ not equals", btit1.posZ, btit2.posZ);
		assertEquals(message + " -> nextY not equals", btit1.nextY, btit2.nextY);
		assertEquals(message + " -> nextZ not equals", btit1.nextZ, btit2.nextZ);

	}

	@Test
	public void jumpTest() throws ParserException, IOException {
		List<TripleString> datasetOrdered = new ArrayList<>();
		LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(1000, 67).withMaxLiteralSize(20)
				.withMaxElementSplit(10).createTripleStringStream().forEachRemaining(datasetOrdered::add);

		TripleComponentOrder[] orders = { TripleComponentOrder.SPO };
		for (TripleComponentOrder order : orders) {
			if (order == TripleComponentOrder.Unknown)
				continue;

			List<TripleString> dataset = datasetOrdered.stream().map(TripleString::tripleToString)
					.peek(ts -> TripleOrderConvert.swapComponentOrder(ts, TripleComponentOrder.SPO, order)).toList();

			List<TripleString> datasetTests = new ArrayList<>(datasetOrdered);

			long jumps = 0;
			try (HDT hdt = HDTManager.generateHDT(dataset.iterator(), HDTTestUtils.BASE_URI, HDTOptions.of(),
					ProgressListener.ignore())) {
				IteratorTripleID it = hdt.getTriples().searchAll();
				while (it.hasNext()) {
					TripleID next = it.next();
					TripleID nextUnorder = next.clone();

					CharSequence s = hdt.getDictionary().idToString(next.getSubject(), TripleComponentRole.SUBJECT);
					CharSequence p = hdt.getDictionary().idToString(next.getPredicate(), TripleComponentRole.PREDICATE);
					CharSequence o = hdt.getDictionary().idToString(next.getObject(), TripleComponentRole.OBJECT);
					TripleString nextStr = new TripleString(s, p, o);

					TripleOrderConvert.swapComponentOrder(next, order, TripleComponentOrder.SPO);
					TripleOrderConvert.swapComponentOrder(nextStr, order, TripleComponentOrder.SPO);

					// System.out.printf("%s (%s)\n", nextStr, next);

					assertTrue("missing triple", datasetTests.remove(nextStr));

					// test jump too far
					it.jumpToSubject(dataset.size() + 1);
					it.jumpToPredicate(dataset.size() + 1);
					it.jumpToObject(dataset.size() + 1);

					// test jump before
					it.jumpToSubject(1);
					it.jumpToPredicate(1);
					it.jumpToObject(1);

					// test jump equals
					it.jumpToSubject(nextUnorder.getSubject());
					it.jumpToPredicate(nextUnorder.getPredicate());
					it.jumpToObject(nextUnorder.getObject());
				}
				assertTrue("dataset not empty", datasetTests.isEmpty());

				// check if jumps are working
				// i + 1 vs i + 2

				int doneMask = 0;
				it = hdt.getTriples().searchAll();
				while (it.hasNext()) {
					TripleID next = it.next();
					long position = it.getLastTriplePosition();
					TripleID nextUnorder = next.clone();

					CharSequence s = hdt.getDictionary().idToString(next.getSubject(), TripleComponentRole.SUBJECT);
					CharSequence p = hdt.getDictionary().idToString(next.getPredicate(), TripleComponentRole.PREDICATE);
					CharSequence o = hdt.getDictionary().idToString(next.getObject(), TripleComponentRole.OBJECT);
					TripleString nextStr = new TripleString(s, p, o);

					TripleOrderConvert.swapComponentOrder(next, order, TripleComponentOrder.SPO);
					TripleOrderConvert.swapComponentOrder(nextStr, order, TripleComponentOrder.SPO);

					IteratorTripleID iteratorTripleID = hdt.getTriples().searchAll();
					iteratorTripleID.goTo(position);
					TripleID locStart = iteratorTripleID.next().clone();
					assertEquals("Error after jump #" + jumps, locStart, nextUnorder);

					if (!iteratorTripleID.hasNext()) {
						continue;
					}
					TripleID locEnd = iteratorTripleID.next();
					long posEnd = iteratorTripleID.getLastTriplePosition();
					assertEquals(position + 1, posEnd);

					TripleComponentRole deltaRole = findHighDelta(locStart, locEnd);

					doneMask |= 1 << deltaRole.ordinal();
					switch (deltaRole) {
					case SUBJECT -> it.jumpToSubject(locEnd.getSubject());// ok
					case PREDICATE -> it.jumpToPredicate(locEnd.getPredicate()); // ok
					case OBJECT -> it.jumpToObject(locEnd.getObject()); // ok
					default -> fail();
					}

					assertTrue(it.hasNext());

					IteratorTripleID iteratorTripleIDBefore = hdt.getTriples().searchAll();
					iteratorTripleIDBefore.goTo(posEnd);
					assertTrue(iteratorTripleIDBefore.hasNext());
					assertStatesEqual("after jump, before next jump#" + jumps + " " + Integer.toBinaryString(doneMask)
							+ "/" + deltaRole, iteratorTripleIDBefore, it);

					assertEquals("Error at jump #" + jumps + " " + Integer.toBinaryString(doneMask) + "/" + deltaRole,
							locEnd, it.next());
					assertEquals(posEnd, it.getLastTriplePosition());
					assertStatesEqual(
							"Error at jump #" + jumps + " " + Integer.toBinaryString(doneMask) + "/" + deltaRole, it,
							iteratorTripleID);
					jumps++;
				}

				assertEquals(1 | 2 | 4, doneMask);

				doneMask = 0;
				it = hdt.getTriples().searchAll();
				while (it.hasNext()) {
					TripleID next = it.next();
					long position = it.getLastTriplePosition();
					TripleID nextUnorder = next.clone();

					CharSequence s = hdt.getDictionary().idToString(next.getSubject(), TripleComponentRole.SUBJECT);
					CharSequence p = hdt.getDictionary().idToString(next.getPredicate(), TripleComponentRole.PREDICATE);
					CharSequence o = hdt.getDictionary().idToString(next.getObject(), TripleComponentRole.OBJECT);
					TripleString nextStr = new TripleString(s, p, o);

					TripleOrderConvert.swapComponentOrder(next, order, TripleComponentOrder.SPO);
					TripleOrderConvert.swapComponentOrder(nextStr, order, TripleComponentOrder.SPO);

					IteratorTripleID iteratorTripleID = hdt.getTriples().searchAll();
					iteratorTripleID.goTo(position);
					TripleID locStart = iteratorTripleID.next().clone();
					assertEquals("Error after jump #" + jumps, locStart, nextUnorder);

					if (!iteratorTripleID.hasNext()) {
						continue;
					}
					int diff;
					if (jumps % 2 == 0) {
						diff = 2;
						iteratorTripleID.next();
						if (!iteratorTripleID.hasNext()) {
							continue;
						}
					} else {
						diff = 1;
					}
					TripleID locEnd = iteratorTripleID.next();
					long posEnd = iteratorTripleID.getLastTriplePosition();
					assertEquals(position + diff, posEnd);

					TripleComponentRole deltaRole = findHighDelta(locStart, locEnd);

					if (deltaRole == TripleComponentRole.SUBJECT)
						continue;
					// if (deltaRole == TripleComponentRole.PREDICATE) continue;
					if (deltaRole == TripleComponentRole.OBJECT)
						continue;

					switch (deltaRole) {
					case SUBJECT -> it.jumpToSubject(locEnd.getSubject());
					case PREDICATE -> it.jumpToPredicate(locEnd.getPredicate());
					case OBJECT -> it.jumpToObject(locEnd.getObject());
					default -> fail();
					}

					assertTrue(it.hasNext());

					IteratorTripleID iteratorTripleIDBefore = hdt.getTriples().searchAll();
					iteratorTripleIDBefore.goTo(posEnd);
					assertTrue(iteratorTripleIDBefore.hasNext());
					assertStatesEqual("after jump, before next jump#" + jumps + " " + Integer.toBinaryString(doneMask)
							+ "/" + deltaRole, iteratorTripleIDBefore, it);

					assertEquals("Error at jump #" + jumps + " " + Integer.toBinaryString(doneMask) + "/" + deltaRole,
							locEnd, it.next());
					assertEquals(
							"bad jump pos from " + position + " " + Integer.toBinaryString(doneMask) + "/" + deltaRole,
							posEnd, it.getLastTriplePosition());
					assertStatesEqual(
							"Error at jump #" + jumps + " " + Integer.toBinaryString(doneMask) + "/" + deltaRole, it,
							iteratorTripleID);
					jumps++;
					doneMask |= 1 << deltaRole.ordinal();
				}

				assertEquals(1 | 2 | 4, doneMask);

			}

		}

	}

}

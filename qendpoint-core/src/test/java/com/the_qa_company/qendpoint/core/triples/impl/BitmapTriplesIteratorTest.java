package com.the_qa_company.qendpoint.core.triples.impl;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap64Big;
import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import com.the_qa_company.qendpoint.core.util.disk.SimpleLongArray;
import org.junit.Before;
import org.junit.Test;

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

	@Test
	public void jumpTest() throws ParserException, IOException, NotFoundException {
		TripleComponentOrder order = TripleComponentOrder.SPO;

		List<TripleString> dataset = Stream.of(
				new TripleString("s1", "p1", "ao1"),
				new TripleString("s1", "p2", "bo2"),
				new TripleString("s1", "p3", "co3"),
				new TripleString("s2", "p1", "ao4"),
				new TripleString("s2", "p2", "bo5"),
				new TripleString("s3", "p1", "ao6")
		)
				.peek(ts -> TripleOrderConvert.swapComponentOrder(ts, TripleComponentOrder.SPO, order))
				.toList();


		try (HDT hdt = HDTManager.generateHDT(
				dataset.iterator(),
				HDTTestUtils.BASE_URI,
				HDTOptions.of(),
				ProgressListener.ignore()
		)) {
			IteratorTripleID it = hdt.getTriples().searchAll();
			while (it.hasNext()) {
				TripleID next = it.next();

				CharSequence s = hdt.getDictionary().idToString(next.getSubject(), TripleComponentRole.SUBJECT);
				CharSequence p = hdt.getDictionary().idToString(next.getPredicate(), TripleComponentRole.PREDICATE);
				CharSequence o = hdt.getDictionary().idToString(next.getObject(), TripleComponentRole.OBJECT);
				TripleString nextStr = new TripleString(s, p, o);

				TripleOrderConvert.swapComponentOrder(next, order, TripleComponentOrder.SPO);
				TripleOrderConvert.swapComponentOrder(nextStr, order, TripleComponentOrder.SPO);

				System.out.printf("%s (%s)\n", nextStr, next);



			}

			// check if jumps are working
			// i + 1 vs i + 2



		}


	}

}

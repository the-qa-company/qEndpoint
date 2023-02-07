package com.the_qa_company.qendpoint.core.tests;

import java.util.Iterator;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIteratorY;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIteratorYFOQ;

public class TriplesTest {

	private static void measure(Iterator<TripleID> it) {
		StopWatch st = new StopWatch();
		long count = 0;
		while (it.hasNext()) {
			it.next();

			if ((count % 1000000) == 0) {
				System.out.println((count / 1000000) + "M");
			}
			count++;
		}
		System.out.println(st.stopAndShow());
	}

	public static void main(String[] args) throws Throwable {
		try (HDT hdt = HDTManager.mapIndexedHDT("/Users/mck/hdt/DBPedia-3.9-en.hdt")) {

			TripleID pat = new TripleID(0, 53201, 0);
			final BitmapTriples t = (BitmapTriples) hdt.getTriples();

			Iterator<TripleID> itA = new BitmapTriplesIteratorYFOQ(t, pat);
			Iterator<TripleID> itB = new BitmapTriplesIteratorY(t, pat);

			measure(itA);

//  		compare(itA,itB);
		}
	}
}

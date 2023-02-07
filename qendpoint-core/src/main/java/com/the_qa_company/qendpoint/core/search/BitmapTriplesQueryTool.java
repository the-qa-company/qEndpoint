package com.the_qa_company.qendpoint.core.search;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.search.query.BitmapTriplesHDTQueryIterator;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;

import java.util.Iterator;

/**
 * Implementation of {@link SimpleQueryTool} using the sorted aspect of the
 * {@link BitmapTriples} of the HDT to compute the join
 *
 * @author Antoine Willerval
 */
public class BitmapTriplesQueryTool extends SimpleQueryTool {
	private final BitmapTriples bitmapTriples;

	public BitmapTriplesQueryTool(HDT hdt) {
		super(hdt);
		assert hdt.getTriples() instanceof BitmapTriples;
		bitmapTriples = (BitmapTriples) hdt.getTriples();
	}

	@Override
	public Iterator<HDTQueryResult> query(HDTQuery q) {
		return new BitmapTriplesHDTQueryIterator(bitmapTriples, getHDT(), q, q.getTimeout());
	}
}

package com.the_qa_company.qendpoint.core.search.query;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.search.HDTQuery;
import com.the_qa_company.qendpoint.core.search.HDTQueryResult;
import com.the_qa_company.qendpoint.core.search.exception.HDTSearchTimeoutException;
import com.the_qa_company.qendpoint.core.search.result.MapHDTQueryResult;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;

public class BitmapTriplesHDTQueryIterator extends FetcherIterator<HDTQueryResult> {
	private final MapHDTQueryResult result = new MapHDTQueryResult();
	private final BitmapTriples triples;
	private final HDT hdt;
	private final HDTQuery query;
	private final long timeout;

	public BitmapTriplesHDTQueryIterator(BitmapTriples triples, HDT hdt, HDTQuery query, long timeout) {
		this.triples = triples;
		this.hdt = hdt;
		this.query = query;
		if (timeout == 0) {
			this.timeout = Long.MAX_VALUE;
		} else {
			long current = System.currentTimeMillis();
			if (Long.MAX_VALUE - timeout > current) {
				this.timeout = current + timeout;
			} else {
				// too big, act like the user is asking an infinite timeout
				this.timeout = Long.MAX_VALUE;
			}
		}
	}

	private void checkTimeout() {
		if (System.currentTimeMillis() > timeout) {
			throw new HDTSearchTimeoutException();
		}
	}

	@Override
	protected HDTQueryResult getNext() {
		checkTimeout();

		return null;
	}
}

package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.OneReadTempTriples;

import java.io.IOException;
import java.util.Iterator;

public class TripleCompressionResultEmpty implements TripleCompressionResult {
	private final TripleComponentOrder order;

	public TripleCompressionResultEmpty(TripleComponentOrder order) {
		this.order = order;
	}

	@Override
	public TempTriples getTriples() {
		return new OneReadTempTriples(new Iterator<>() {
			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public TripleID next() {
				return null;
			}
		}, order, 0, 0);
	}

	@Override
	public long getTripleCount() {
		return 0;
	}

	@Override
	public void close() throws IOException {

	}
}

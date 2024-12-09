package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.NoSuchElementException;

public class EmptyTriplesIterator implements SuppliableIteratorTripleID {
	private final TripleComponentOrder order;

	public EmptyTriplesIterator(TripleComponentOrder order) {
		this.order = order;
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public TripleID next() {
		throw new NoSuchElementException();
	}

	@Override
	public void goToStart() {
		// Do nothing
	}

	@Override
	public boolean canGoTo() {
		return false;
	}

	@Override
	public void goTo(long pos) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long estimatedNumResults() {
		return 0;
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.EXACT;
	}

	@Override
	public TripleComponentOrder getOrder() {
		return order;
	}

	@Override
	public long getLastTriplePosition() {
		throw new NoSuchElementException();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}

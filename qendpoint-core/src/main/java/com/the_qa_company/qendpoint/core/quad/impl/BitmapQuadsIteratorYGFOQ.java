package com.the_qa_company.qendpoint.core.quad.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIteratorYFOQ;

public class BitmapQuadsIteratorYGFOQ implements SuppliableIteratorTripleID {

	// resolves ?P?G queries
	private final Bitmap bitmapGraph; // the bitmap of the requested graph
	private final BitmapTriplesIteratorYFOQ inIt;
	private TripleID nextRes = null;

	public BitmapQuadsIteratorYGFOQ(BitmapTriples triples, TripleID pattern) {
		this.inIt = new BitmapTriplesIteratorYFOQ(triples, pattern);
		this.bitmapGraph = triples.getQuadInfoAG().get((int) pattern.getGraph() - 1);
		this.goToStart();
		this.calculateNext();
	}

	private boolean isValidZ() {
		return this.inIt.getPosZ() != -1 && this.bitmapGraph.access(this.inIt.getPosZ() - 1);
	}

	@Override
	public void goToStart() {
		this.inIt.goToStart();
	}

	@Override
	public boolean hasNext() {
		return this.nextRes != null;
	}

	private void calculateNext() {
		this.nextRes = null;
		while (this.inIt.hasNext()) {
			TripleID next = this.inIt.next().clone();
			if (!this.isValidZ())
				continue;
			this.nextRes = next;
			break;
		}
	}

	@Override
	public TripleID next() {
		TripleID res = this.nextRes.clone();
		this.calculateNext();
		return res;
	}

	@Override
	public boolean hasPrevious() {
		return this.inIt.hasPrevious();
	}

	@Override
	public TripleID previous() {
		return this.inIt.previous();
	}

	@Override
	public boolean canGoTo() {
		return this.inIt.canGoTo();
	}

	@Override
	public void goTo(long pos) {
		this.inIt.goTo(pos);
	}

	@Override
	public long estimatedNumResults() {
		return this.inIt.estimatedNumResults();
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return this.inIt.numResultEstimation();
	}

	@Override
	public TripleComponentOrder getOrder() {
		return this.inIt.getOrder();
	}

	@Override
	public long getLastTriplePosition() {
		return this.inIt.getLastTriplePosition();
	}
}

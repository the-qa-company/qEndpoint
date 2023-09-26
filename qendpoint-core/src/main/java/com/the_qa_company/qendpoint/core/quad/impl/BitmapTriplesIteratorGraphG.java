package com.the_qa_company.qendpoint.core.quad.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapQuadTriples;

public class BitmapTriplesIteratorGraphG extends FetcherIterator<TripleID> implements SuppliableIteratorTripleID {
	private final long graph;
	private final Bitmap bitmapW;
	protected final long minZ, maxZ;
	protected final TripleID qid = new TripleID();
	protected final BitmapQuadTriples triples;
	protected long posZ;

	public BitmapTriplesIteratorGraphG(BitmapQuadTriples triples, TripleID pattern) {
		this.triples = triples;
		this.graph = pattern.getGraph();

		bitmapW = triples.getQuadInfoAG().get((int) (graph - 1));

		minZ = bitmapW.select1(1);
		maxZ = bitmapW.select1(bitmapW.countOnes());

		goToStart();
	}

	@Override
	protected TripleID getNext() {
		if (posZ == maxZ) {
			return null;
		}

		if (posZ == -1) {
			posZ = minZ; // start
		} else {
			posZ = bitmapW.select1(bitmapW.rank1(posZ) + 1); // next
		}

		TripleID tripleID = triples.findTriple(posZ, qid);
		tripleID.setGraph(graph);
		return tripleID;
	}

	@Override
	public boolean hasPrevious() {
		throw new NotImplementedException();
	}

	@Override
	public TripleID previous() {
		throw new NotImplementedException();
	}

	@Override
	public void goToStart() {
		posZ = -1;
	}

	@Override
	public boolean canGoTo() {
		return false;
	}

	@Override
	public void goTo(long pos) {
		throw new NotImplementedException();
	}

	@Override
	public long estimatedNumResults() {
		return bitmapW.rank1(maxZ) - bitmapW.rank1(minZ) + 1;
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.EXACT;
	}

	@Override
	public TripleComponentOrder getOrder() {
		return triples.getOrder();
	}

	@Override
	public long getLastTriplePosition() {
		return posZ;
	}
}

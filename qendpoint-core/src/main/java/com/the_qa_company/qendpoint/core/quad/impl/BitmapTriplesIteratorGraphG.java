package com.the_qa_company.qendpoint.core.quad.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.MultiLayerBitmap;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapQuadTriples;

public class BitmapTriplesIteratorGraphG extends FetcherIterator<TripleID> implements SuppliableIteratorTripleID {
	private final long graph;
	private final MultiLayerBitmap mlb;
	private final long posW;
	protected final long minZ, maxZ;
	protected final TripleID qid = new TripleID();
	protected final BitmapQuadTriples triples;
	protected long posZ;

	public BitmapTriplesIteratorGraphG(BitmapQuadTriples triples, TripleID pattern) {
		this.triples = triples;
		this.graph = pattern.getGraph();

		mlb = triples.getQuadInfoAG();

		posW = graph - 1;

		minZ = mlb.select1(posW, 1);
		maxZ = mlb.select1(posW, mlb.countOnes(posW));

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
			posZ = mlb.select1(posW, mlb.rank1(posW, posZ) + 1); // next
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
		return mlb.rank1(posW, maxZ) - mlb.rank1(posW, minZ) + 1;
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

package com.the_qa_company.qendpoint.core.quad.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapQuadTriples;

import java.util.List;

public class BitmapTriplesIteratorGraph extends FetcherIterator<TripleID> implements SuppliableIteratorTripleID {
	private final BitmapQuadTriples quads;
	private final IteratorTripleID tidIt;
	private TripleID tid;
	private long posZ;
	private final long graph;

	public BitmapTriplesIteratorGraph(BitmapQuadTriples triples, IteratorTripleID tid, long graph) {
		this.quads = triples;
		this.tidIt = tid;
		this.graph = graph;
	}

	@Override
	protected TripleID getNext() {
		List<? extends Bitmap> quadInfoAG = quads.getQuadInfoAG();
		while (true) {
			if (tid == null) { // we need to compute the next one
				if (!tidIt.hasNext()) {
					return null;
				}
				// get the last TID
				tid = tidIt.next();
				tid.setGraph(graph);
				posZ = tidIt.getLastTriplePosition();
			}

			if (graph != 0) {
				// we are searching for a particular graph, we only need to check if this graph
				// contains the current triple
				if (quadInfoAG.get((int) graph - 1).access(posZ)) {
					TripleID id = tid;
					tid = null; // pass to the next one in the future case
					return id;
				}
				// search another
				continue;
			}

			for (long i = tid.getGraph() + 1; i <= quadInfoAG.size(); i++) {
				if (quadInfoAG.get((int) i - 1).access(posZ)) {
					// found a graph containing it
					tid.setGraph(i);
					return tid;
				}
			}
			tid = null; // pass to the next one
		}
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
		tidIt.goToStart();
		tid = null;
	}

	@Override
	public boolean canGoTo() {
		return tidIt.canGoTo();
	}

	@Override
	public void goTo(long pos) {
		tidIt.goTo(pos);
		tid = null;
	}

	@Override
	public long estimatedNumResults() {
		return tidIt.estimatedNumResults() * quads.size();
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.UP_TO;
	}

	@Override
	public TripleComponentOrder getOrder() {
		return tidIt.getOrder();
	}

	@Override
	public long getLastTriplePosition() {
		return posZ;
	}
}

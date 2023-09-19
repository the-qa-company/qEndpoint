package com.the_qa_company.qendpoint.core.quad.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIteratorYFOQ;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;

public class BitmapQuadsIteratorYFOQ implements SuppliableIteratorTripleID {

	private final BitmapTriplesIteratorYFOQ inIt;
	private final List<? extends Bitmap> bitmapGraphs;
	private List<Long> graphs;
	private TripleID curTriple;

	public BitmapQuadsIteratorYFOQ(BitmapTriples triples, TripleID pattern) {
		this.inIt = new BitmapTriplesIteratorYFOQ(triples, pattern);
		this.bitmapGraphs = triples.getQuadInfoAG();
		this.graphs = new ArrayList<>();
	}

	private void updateNextTriple() {
		if (!this.inIt.hasNext())
			throw new RuntimeException("inIt should have next");
		this.curTriple = this.inIt.next();
		this.graphs = bitmapGraphs.stream().parallel().filter(graph -> graph.access((int) this.inIt.getPosZ() - 1))
				.map(graph -> bitmapGraphs.indexOf(graph) + 1L).collect(Collectors.toList());
	}

	@Override
	public boolean hasNext() {
		return this.graphs.size() > 0 || this.inIt.hasNext();
	}

	@Override
	public TripleID next() {
		if (graphs.isEmpty()) {
			this.updateNextTriple();
			return this.next();
		}
		long curGraph = graphs.remove(0);
		TripleID curTriple = this.curTriple.clone();
		curTriple.setGraph(curGraph);
		return curTriple;
	}

	@Override
	public void goToStart() {
		this.inIt.goToStart();
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

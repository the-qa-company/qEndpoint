package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;

public class GraphFilteringTripleId implements IteratorTripleID {
	private final IteratorTripleID iterator;
	private final long[] graphIds;
	private TripleID next;

	public GraphFilteringTripleId(IteratorTripleID iterator, long[] graphIds) {
		this.iterator = iterator;
		this.graphIds = graphIds;
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
		throw new NotImplementedException();
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
		return iterator.estimatedNumResults();
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return iterator.numResultEstimation();
	}

	@Override
	public TripleComponentOrder getOrder() {
		return iterator.getOrder();
	}

	@Override
	public long getLastTriplePosition() {
		return iterator.getLastTriplePosition();
	}

	@Override
	public boolean isLastTriplePositionBoundToOrder() {
		return iterator.isLastTriplePositionBoundToOrder();
	}

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
		}
		while (iterator.hasNext()) {
			TripleID val = iterator.next();

			long g = val.getGraph();
			for (long graphId : graphIds) {
				if (graphId == g) {
					next = val;
					return true;
				}
			}
			// can't find valid graph
		}
		return false;
	}

	@Override
	public TripleID next() {
		if (!hasNext()) {
			return null;
		}
		TripleID newVal = next;
		next = null;
		return newVal;
	}

	@Override
	public void remove() {
		throw new NotImplementedException();
	}
}

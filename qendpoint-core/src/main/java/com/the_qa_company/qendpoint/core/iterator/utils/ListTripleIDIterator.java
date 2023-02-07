package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.List;

import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;

/**
 * Iterator implementation to iterate over a List&lt;TripleID&gt; object
 *
 * @author mario.arias
 */
public class ListTripleIDIterator implements IteratorTripleID {

	private final List<TripleID> triplesList;
	private int pos;
	private long lastPosition;

	public ListTripleIDIterator(List<TripleID> triplesList) {
		this.triplesList = triplesList;
		this.pos = 0;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return pos < triplesList.size();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#next()
	 */
	@Override
	public TripleID next() {
		lastPosition = pos;
		return triplesList.get(pos++);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasPrevious()
	 */
	@Override
	public boolean hasPrevious() {
		return pos > 0;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#previous()
	 */
	@Override
	public TripleID previous() {
		TripleID tripleID = triplesList.get(--pos);
		lastPosition = pos;
		return tripleID;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#goToStart()
	 */
	@Override
	public void goToStart() {
		pos = 0;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#estimatedNumResults()
	 */
	@Override
	public long estimatedNumResults() {
		return triplesList.size();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#numResultEstimation()
	 */
	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.EXACT;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#canGoTo()
	 */
	@Override
	public boolean canGoTo() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#goTo(int)
	 */
	@Override
	public void goTo(long pos) {
		this.pos = (int) pos;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#getOrder()
	 */
	@Override
	public TripleComponentOrder getOrder() {
		return TripleComponentOrder.Unknown;
	}

	@Override
	public long getLastTriplePosition() {
		return lastPosition;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}

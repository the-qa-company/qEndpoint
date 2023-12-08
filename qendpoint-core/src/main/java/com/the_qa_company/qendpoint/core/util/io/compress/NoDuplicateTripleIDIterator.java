package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;

/**
 * a iterator triple id implementation remove duplicated
 *
 * @author Antoine Willerval
 */
public class NoDuplicateTripleIDIterator implements IteratorTripleID {
	private TripleID next;
	private final TripleID prev = new TripleID(-1, -1, -1, -1);
	private final IteratorTripleID it;

	public NoDuplicateTripleIDIterator(IteratorTripleID it) {
		this.it = it;
	}

	@Override
	public boolean hasNext() {
		while (this.next == null) {
			if (!it.hasNext()) {
				return false;
			}

			TripleID next = it.next();

			if (next.equals(prev)) {
				continue;
			}
			prev.setAll(next.getSubject(), next.getPredicate(), next.getObject(), next.getGraph());

			this.next = next;
		}
		return true;
	}

	@Override
	public TripleID next() {
		if (!hasNext()) {
			return null;
		}
		TripleID next = this.next;
		this.next = null;
		return next;
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
		return it.estimatedNumResults();
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return it.numResultEstimation();
	}

	@Override
	public TripleComponentOrder getOrder() {
		return it.getOrder();
	}

	@Override
	public long getLastTriplePosition() {
		throw new NotImplementedException();
	}
}

package com.the_qa_company.qendpoint.core.iterator;

import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;

public interface SuppliableIteratorTripleID extends IteratorTripleID {

	/**
	 * create a supplier for the position (if compute the position isn't made in
	 * O(1), the implementation should override this method)
	 *
	 * @return the supplier
	 */
	default TriplePositionSupplier getLastTriplePositionSupplier() {
		return TriplePositionSupplier.of(getLastTriplePosition());
	}
}

package com.the_qa_company.qendpoint.core.storage.search;

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.storage.QEPDataset;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.Iterator;

public class QEPDatasetIterator implements Iterator<QEPComponentTriple> {
	private final QEPDataset dataset;
	private final IteratorTripleID iterator;
	private final QEPComponentTriple triple;
	private boolean next;

	public QEPDatasetIterator(QEPDataset dataset, IteratorTripleID iterator, QEPComponentTriple triple) {
		this.dataset = dataset;
		this.iterator = iterator;
		this.triple = triple;
	}

	@Override
	public boolean hasNext() {
		if (next) {
			return true;
		}
		while (iterator.hasNext()) {
			TripleID tid = iterator.next();
			long position = iterator.getLastTriplePosition();

			if (dataset.isTripleDeleted(position)) {
				// the triple is deleted, we can ignore it
				continue;
			}

			// small overhead with the frozen components, but negligible (?)
			triple.setAll(
					dataset.component(tid.getSubject(), TripleComponentRole.SUBJECT),
					dataset.component(tid.getPredicate(), TripleComponentRole.PREDICATE),
					dataset.component(tid.getObject(), TripleComponentRole.OBJECT),
					position
			);

			return true;
		}
		// no more triple
		return false;
	}

	@Override
	public QEPComponentTriple next() {
		if (!hasNext()) {
			return null;
		}
		try {
			return triple;
		} finally {
			next = false;
		}
	}
}

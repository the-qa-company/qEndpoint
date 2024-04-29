package com.the_qa_company.qendpoint.core.storage.search;

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.storage.QEPCore;
import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import com.the_qa_company.qendpoint.core.storage.QEPDataset;
import com.the_qa_company.qendpoint.core.storage.QEPDatasetContext;
import com.the_qa_company.qendpoint.core.storage.iterator.QueryCloseableIterator;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;

/**
 * Iterator over a dataset, will handle the triple deletion
 *
 * @author Antoine Willerval
 */
public class QEPDatasetIterator implements QueryCloseableIterator {
	private final QEPDatasetContext ctx;
	private final IteratorTripleID iterator;
	private final QEPComponentTriple triple;
	private boolean next;

	public QEPDatasetIterator(QEPDatasetContext ctx, IteratorTripleID iterator, QEPComponentTriple triple) {
		this.ctx = ctx;
		this.iterator = iterator;
		this.triple = triple;
	}

	@Override
	public boolean hasNext() {
		if (next) {
			return true;
		}
		QEPDataset ds = ctx.dataset();
		while (iterator.hasNext()) {
			TripleID tid = iterator.next();

			if (ctx.hasDeletions()) {
				long position = iterator.getLastTriplePosition();

				if (ctx.isTripleDeleted(position)) {
					// the triple is deleted, we can ignore it
					continue;
				}
			}

			// small overhead with the frozen components, but negligible (?)
			triple.setAll(ds.component(tid.getSubject(), TripleComponentRole.SUBJECT),
					ds.component(tid.getPredicate(), TripleComponentRole.PREDICATE),
					ds.component(tid.getObject(), TripleComponentRole.OBJECT), ds.uid());
			next = true;

			return true;
		}
		// no more triple
		return false;
	}

	@Override
	public void remove() {
		ctx.dataset().deleteTriple(lastId());
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

	@Override
	public long lastId() {
		return iterator.getLastTriplePosition();
	}

	@Override
	public void close() throws QEPCoreException {
		// no context to close because it was built using the
		// CatCloseableIterator
	}

	@Override
	public long estimateCardinality() {
		return iterator.estimatedNumResults();
	}
}

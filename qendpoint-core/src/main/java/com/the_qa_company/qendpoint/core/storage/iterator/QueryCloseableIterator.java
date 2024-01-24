package com.the_qa_company.qendpoint.core.storage.iterator;

import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;

/**
 * query closeable iterator
 *
 * @author Antoine Willerval
 */
public interface QueryCloseableIterator extends CloseableIterator<QEPComponentTriple> {
	static QueryCloseableIterator empty() {
		return new QueryCloseableIterator() {
			@Override
			public long estimateCardinality() {
				return 0;
			}

			@Override
			public void close() throws QEPCoreException {

			}

			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public QEPComponentTriple next() {
				return null;
			}
		};
	}

	/**
	 * @return an estimation of the cardinality of the search
	 */
	long estimateCardinality();

	@Override
	default QueryCloseableIterator attach(AutoCloseableGeneric<? extends RuntimeException> closeable) {
		return CloseableAttachQueryIterator.of(this, closeable);
	}
}

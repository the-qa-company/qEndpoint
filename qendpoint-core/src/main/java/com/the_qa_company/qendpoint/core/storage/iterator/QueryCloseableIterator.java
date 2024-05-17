package com.the_qa_company.qendpoint.core.storage.iterator;

import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;

/**
 * query closeable iterator
 *
 * @author Antoine Willerval
 */
public interface QueryCloseableIterator extends CloseableIterator<QEPComponentTriple, QEPCoreException> {
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

			@Override
			public long lastId() {
				return 0;
			}
		};
	}

	/**
	 * @return an estimation of the cardinality of the search
	 */
	long estimateCardinality();

	/**
	 * @return get last id
	 */
	long lastId();

	@Override
	default QueryCloseableIterator attach(AutoCloseableGeneric<QEPCoreException> closeable) {
		return CloseableAttachQueryIterator.of(this, closeable);
	}
}

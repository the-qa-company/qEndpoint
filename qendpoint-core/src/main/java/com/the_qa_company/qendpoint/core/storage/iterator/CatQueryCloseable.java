package com.the_qa_company.qendpoint.core.storage.iterator;

import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;

import java.util.List;

/**
 * Version of {@link CatQueryCloseable} in {@link CloseableIterator} context
 *
 * @author Antoine Willerval
 */
public class CatQueryCloseable extends FetcherCloseableIterator<QEPComponentTriple, QEPCoreException>
		implements QueryCloseableIterator {
	/**
	 * create iterator
	 *
	 * @param its iterators
	 * @return iterator
	 */
	public static QueryCloseableIterator of(QueryCloseableIterator... its) {
		return of(List.of(its));
	}

	/**
	 * create iterator
	 *
	 * @param its iterators
	 * @return iterator
	 */
	public static QueryCloseableIterator of(List<QueryCloseableIterator> its) {
		// handle easy cases
		if (its.isEmpty()) {
			return QueryCloseableIterator.empty();
		}
		if (its.size() == 1) {
			return its.get(0);
		}
		return new CatQueryCloseable(its);
	}

	private final List<QueryCloseableIterator> iterators;
	private int index;

	private CatQueryCloseable(List<QueryCloseableIterator> iterators) {
		this.iterators = iterators;
	}

	@Override
	protected QEPComponentTriple getNext() {
		while (index < iterators.size()) {
			if (iterators.get(index).hasNext()) {
				return iterators.get(index).next();
			}
			index++;
		}
		return null;
	}

	@Override
	public void close() throws QEPCoreException {
		AutoCloseableGeneric.closeAll(iterators);
	}

	@Override
	public long estimateCardinality() {
		return iterators.stream().mapToLong(QueryCloseableIterator::estimateCardinality).sum();
	}

	@Override
	public long lastId() {
		if (iterators.size() == index) {
			return 0;
		}
		return iterators.get(index).lastId();
	}
}

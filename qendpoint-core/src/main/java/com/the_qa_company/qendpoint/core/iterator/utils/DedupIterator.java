package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;

public class DedupIterator<T> extends PrefetchIterator<T> {
	Iterator<? extends T> it;
	T last;

	public DedupIterator(Iterator<? extends T> it) {
		this.it = it;
	}

	@Override
	protected T prefetch() {

		while (it.hasNext()) {
			T newVal = it.next();
			if (!newVal.equals(last)) {
				last = newVal;
				return newVal;
			}
		}
		return null;
	}

}

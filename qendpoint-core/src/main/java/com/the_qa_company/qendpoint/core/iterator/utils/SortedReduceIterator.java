package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SortedReduceIterator<T> implements Iterator<T> {

	final Iterator<T> it;
	final Reducer<T> reducer;
	T nextValue;

	public SortedReduceIterator(Iterator<T> it, Reducer<T> reducer) {
		this.it = it;
		this.reducer = reducer;

		if (it.hasNext()) {
			nextValue = it.next();
		}
	}

	@Override
	public boolean hasNext() {
		return nextValue != null;
	}

	@Override
	public T next() {
		if (nextValue == null) {
			throw new NoSuchElementException();
		}

		T returned = nextValue;

		while (true) {
			if (it.hasNext()) {
				nextValue = it.next();

				T reduced = reducer.reduce(returned, nextValue);
				if (reduced != null) {
					returned = reduced;
				} else {
					break;
				}
			} else {
				nextValue = null;
				break;
			}
		}

		return returned;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}

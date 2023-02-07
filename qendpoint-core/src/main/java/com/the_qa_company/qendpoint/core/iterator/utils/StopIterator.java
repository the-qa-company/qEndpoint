package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Predicate;

public class StopIterator<T> implements Iterator<T> {
	private final Iterator<? extends T> it;
	private T next;
	private final Predicate<T> stop;

	public StopIterator(Iterator<? extends T> it, Predicate<T> stop) {
		this.it = Objects.requireNonNull(it, "it can't be null!");
		this.stop = Objects.requireNonNull(stop, "stop can't be null!");
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
			if (!it.hasNext()) {
				return false;
			}
			next = it.next();
		}
		return stop.test(next);
	}

	@Override
	public T next() {
		if (!hasNext()) {
			return null;
		}
		try {
			return next;
		} finally {
			next = null;
		}
	}
}

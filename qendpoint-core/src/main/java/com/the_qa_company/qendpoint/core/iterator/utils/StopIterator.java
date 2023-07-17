package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Predicate;

public class StopIterator<T> implements PeekIterator<T> {
	private final Iterator<? extends T> it;
	private T next;
	private final Predicate<T> stop;

	public StopIterator(Iterator<? extends T> it, Predicate<T> stop) {
		this.it = Objects.requireNonNull(it, "it can't be null!");
		this.stop = Objects.requireNonNull(stop, "stop can't be null!");
	}

	@Override
	public T peek() {
		if (!hasNext()) {
			return null;
		}
		return next;
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
		try {
			return peek();
		} finally {
			next = null;
		}
	}
}

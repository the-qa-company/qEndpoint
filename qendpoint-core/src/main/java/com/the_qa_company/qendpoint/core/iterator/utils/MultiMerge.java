package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class MultiMerge<T> implements Iterator<T> {
	final PriorityQueue<Source<T>> queue = new PriorityQueue<>();

	private static class Source<T> implements Comparable<Source<T>> {
		final Comparator<T> comparator;
		final Iterator<T> it;
		T entry;

		Source(Iterator<T> it, Comparator<T> comparator) {
			this.it = it;
			this.comparator = comparator;
		}

		public boolean read() {
			if (it.hasNext()) {
				entry = it.next();
				return true;
			} else {
				return false;
			}
		}

		@Override
		public int compareTo(Source<T> o) {
			return comparator.compare(this.entry, o.entry);
		}

	}

	public MultiMerge(Iterator<Iterator<T>> it, Comparator<T> comparator) {
		while (it.hasNext()) {
			Iterator<T> inner = it.next();

			if (inner.hasNext()) {
				Source<T> s = new Source<>(inner, comparator);
				if (s.read()) {
					queue.add(s);
				}
			}
		}
	}

	@Override
	public boolean hasNext() {
		return !queue.isEmpty();
	}

	@Override
	public T next() {
		Source<T> item = queue.remove();

		T out = item.entry;

		if (item.read()) {
			queue.add(item);
		}

		return out;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}

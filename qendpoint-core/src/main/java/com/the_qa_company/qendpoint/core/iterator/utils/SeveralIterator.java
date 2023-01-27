package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Collections;
import java.util.Iterator;

/**
 * Receives an iterator of Keys, for each key it uses the IteratorGetter to get
 * an iterator of values associated to that key. The output iterator goes
 * through all values of all keys. The IteratorGetter returns an iterator given
 * a specific key.
 *
 * @author Mario Arias
 * @param <K>
 * @param <T>
 */
public class SeveralIterator<K, T> extends PrefetchIterator<T> {
	public interface IteratorGetter<K, T> {
		Iterator<T> get(K k);
	}

	IteratorGetter<K, T> getter;
	Iterator<K> input;
	Iterator<T> child = Collections.emptyIterator();

	public SeveralIterator(Iterator<K> input, IteratorGetter<K, T> getter) {
		this.input = input;
		this.getter = getter;

	}

	@Override
	protected T prefetch() {
		if (child.hasNext()) {
			return child.next();
		}

		while (input.hasNext()) {
			K k = input.next();
			child = getter.get(k);
			if (child.hasNext()) {
				return child.next();
			}
		}
		return null;
	}

}

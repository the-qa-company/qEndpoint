package com.the_qa_company.qendpoint.core.storage.iterator;

import java.util.ArrayList;
import java.util.List;

public class CatCloseableIterator<T, E extends Exception> extends FetcherCloseableIterator<T, E> {
	/**
	 * create iterator
	 *
	 * @param its iterators
	 * @param <E> iterator type
	 * @return iterator
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <T, E extends Exception> CloseableIterator<? extends T, E> of(
			CloseableIterator<? extends T, E>... its) {
		return of(List.of(its));
	}

	/**
	 * create iterator
	 *
	 * @param its iterators
	 * @param <E> iterator type
	 * @return iterator
	 */
	public static <T, E extends Exception> CloseableIterator<? extends T, E> of(
			List<CloseableIterator<? extends T, E>> its) {
		// handle easy cases
		if (its.isEmpty()) {
			return new EmptyCloseableIteration<>();
		}
		if (its.size() == 1) {
			return its.get(0);
		}
		return new CatCloseableIterator<>(its);
	}

	private final List<? extends CloseableIterator<? extends T, E>> iterators;
	private int index;

	private CatCloseableIterator(List<? extends CloseableIterator<? extends T, E>> iterators) {
		this.iterators = iterators;
	}

	@Override
	protected T getNext() {
		while (index < iterators.size()) {
			if (iterators.get(index).hasNext()) {
				return iterators.get(index).next();
			}
			index++;
		}
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void close() throws E {
		List<Throwable> l = new ArrayList<>(0);
		for (CloseableIterator<? extends T, E> it : iterators) {
			try {
				it.close();
			} catch (Throwable t) {
				l.add(t);
			}
		}
		if (l.isEmpty()) {
			return; // all good
		}

		// search error
		l.stream().filter(t -> t instanceof Error).findAny().ifPresent(t -> {
			l.stream().filter(t2 -> t2 != t).forEach(t::addSuppressed);
			throw (Error) t;
		});
		// search re
		l.stream().filter(t -> t instanceof RuntimeException).findAny().ifPresent(t -> {
			l.stream().filter(t2 -> t2 != t).forEach(t::addSuppressed);
			throw (RuntimeException) t;
		});
		// search E
		E e;
		try {
			e = (E) l.get(0);
		} catch (ClassCastException ee) {
			Error err = new Error("close operation returned an exception that isn't an E/Error/RuntimeException", ee);
			for (int i = 1; i < l.size(); i++) {
				err.addSuppressed(l.get(i));
			}
			throw err;
		}
		for (int i = 1; i < l.size(); i++) {
			e.addSuppressed(l.get(i));
		}
		throw e;
	}
}

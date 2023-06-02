package com.the_qa_company.qendpoint.core.storage.iterator;

import java.util.ArrayList;
import java.util.List;

/**
 * generic auto closeable
 *
 * @param <E> exception type
 * @author Antoine Willerval
 */
@FunctionalInterface
public interface AutoCloseableGeneric<E extends Exception> extends AutoCloseable {
	/**
	 * close all the elements of an Iterable of AutoCloseable, will assume that
	 * the auto closeables will only throw E, Error or RuntimeException.
	 *
	 * @param elements elements to close
	 * @param <E>      close exception
	 * @throws E close exception
	 */
	@SuppressWarnings("unchecked")
	static <E extends Exception> void closeAll(Iterable<? extends AutoCloseable> elements) throws E {
		List<Throwable> l = new ArrayList<>(0);
		for (AutoCloseable it : elements) {
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

	@Override
	void close() throws E;
}

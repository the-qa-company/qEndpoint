package com.the_qa_company.qendpoint.store.experimental.model;

import com.the_qa_company.qendpoint.core.storage.iterator.CloseableIterator;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings("deprecation")
public class QEPCloseableIteration<T> implements CloseableIteration<T> {

	public static <T> QEPCloseableIteration<T> of(CloseableIterator<T> it) {
		return new QEPCloseableIteration<T>(Objects.requireNonNull(it, "it can't be null!"));
	}

	public static <T> QEPCloseableIteration<T> of() {
		return of(CloseableIterator.empty());
	}

	private final CloseableIterator<T> it;

	private QEPCloseableIteration(CloseableIterator<T> it) {
		this.it = it;
	}

	@Override
	public void close() {
		it.close();
	}

	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public T next() {
		return it.next();
	}

	@Override
	public void remove() {
		it.remove();
	}

	public <U, E2 extends RuntimeException> QEPCloseableIteration<U> map(Function<T, U> mapElem,
			Function<Throwable, E2> map) {
		return new QEPCloseableIteration<>(new CloseableIterator<>() {
			@Override
			public void close() {
				try {
					it.close();
				} catch (Throwable t) {
					if (t instanceof Error err) {
						throw err;
					}
					E2 e = map.apply(t);
					if (e != null) {
						throw e;
					}
					if (t instanceof RuntimeException re) {
						throw re;
					}
					throw new AssertionError(t);
				}
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public U next() {
				return mapElem.apply(it.next());
			}

			@Override
			public void remove() {
				it.remove();
			}

			@Override
			public void forEachRemaining(Consumer<? super U> action) {
				it.forEachRemaining((e) -> action.accept(mapElem.apply(e)));
			}
		});
	}
}

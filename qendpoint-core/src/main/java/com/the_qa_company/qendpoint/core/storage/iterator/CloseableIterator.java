package com.the_qa_company.qendpoint.core.storage.iterator;

import com.the_qa_company.qendpoint.core.iterator.utils.EmptyIterator;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * {@link Iterator} linked with an {@link AutoCloseable}
 *
 * @param <T> Iterator type
 * @param <E> AutoCloseable close exception
 * @author Antoine Willerval
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseableGeneric<Exception> {
	/**
	 * create a closeable iterator from an iterator, if the iterator is an
	 * {@link AutoCloseable}, it will assume that E is the only thrown
	 * exception, otherwise it'll create a closeable iterator without any close
	 * action.
	 *
	 * @param it  iterator
	 * @param <T> iterator type
	 * @param <E> close exception
	 * @return closeable iterator
	 */
	@SuppressWarnings("unchecked")
	static <T> CloseableIterator<T> of(Iterator<T> it) {
		if (it instanceof AutoCloseable) {
			return of(it, () -> {
				try {
					((AutoCloseable) it).close();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		}
		return of(it, () -> {});
	}

	/**
	 * create a closeable iterator from an iterator and a close operation
	 *
	 * @param it             iterator
	 * @param closeOperation close operation
	 * @param <T>            iterator type
	 * @param <E>            close exception
	 * @return closeable iterator
	 */
	static <T> CloseableIterator<T> of(Iterator<T> it, AutoCloseableGeneric<RuntimeException> closeOperation) {
		return new CloseableIterator<>() {
			@Override
			public void close() {
				closeOperation.close();
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

			@Override
			public void forEachRemaining(Consumer<? super T> action) {
				it.forEachRemaining(action);
			}
		};
	}

	/**
	 * @param <T> iterator type
	 * @param <E> close exception
	 * @return empty closeable iterator
	 */
	static <T, E extends Exception> CloseableIterator<T> empty() {
		return empty(() -> {});
	}

	/**
	 * @param closeable close operation
	 * @param <T>       iterator type
	 * @param <E>       close exception
	 * @return empty closeable iterator
	 */
	static <T> CloseableIterator<T> empty(AutoCloseableGeneric<RuntimeException> closeable) {
		return of(new EmptyIterator<>(), closeable);
	}

	@Override
	void close();

	/**
	 * attach an auto closeable element to this iterator
	 *
	 * @param closeable closeable element
	 * @return new iterator with the new closeable element
	 */
	default CloseableIterator<T> attach(AutoCloseableGeneric<? extends RuntimeException> closeable) {
		return CloseableAttachIterator.of(this, closeable);
	}
}

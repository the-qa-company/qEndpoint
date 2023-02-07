package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * alternative iterator with exception throwing
 *
 * @param <T> the iterator type
 * @param <E> the allowed exception
 * @author Antoine Willerval
 */
public interface ExceptionIterator<T, E extends Exception> {
	@FunctionalInterface
	interface ExceptionConsumer<T, E extends Exception> {
		void consume(T element) throws E;
	}

	/**
	 * create an exception iterator from a basic iterator
	 *
	 * @param it  the iterator the wrap
	 * @param <T> the iterator type
	 * @param <E> the exception to allow
	 * @return exception iterator
	 */
	static <T, E extends Exception> ExceptionIterator<T, E> of(final Iterator<T> it) {
		return new ExceptionIterator<>() {
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
		};
	}

	/**
	 * create an empty iterator
	 *
	 * @param <T> the iterator type
	 * @param <E> the exception to allow
	 * @return exception iterator
	 */
	static <T, E extends Exception> ExceptionIterator<T, E> empty() {
		return of(new Iterator<>() {
			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public T next() {
				return null;
			}
		});
	}

	/**
	 * @return if the iterator has a next element
	 * @throws E exception triggered by the implementation
	 */
	boolean hasNext() throws E;

	/**
	 * @return the next iterator element
	 * @throws E exception triggered by the implementation
	 */
	T next() throws E;

	/**
	 * remove the last element returned by the iterator
	 *
	 * @throws E exception triggered by the implementation
	 */
	default void remove() throws E {
		throw new UnsupportedOperationException("remove");
	}

	/**
	 * loop over all the elements
	 *
	 * @param action the action to handle the element
	 * @throws E exception triggered by the implementation
	 */
	default void forEachRemaining(ExceptionConsumer<? super T, E> action) throws E {
		Objects.requireNonNull(action);
		while (hasNext())
			action.consume(next());
	}

	/**
	 * map this iterator with a function
	 *
	 * @param mappingFunc the mapping function
	 * @param <M>         the new iterator type
	 * @return iterator
	 */
	default <M> ExceptionIterator<M, E> map(MapExceptionIterator.ExceptionFunction<T, M, E> mappingFunc) {
		return new MapExceptionIterator<>(this, mappingFunc);
	}

	/**
	 * map this iterator with a function
	 *
	 * @param mappingFunc the mapping function
	 * @param <M>         the new iterator type
	 * @return iterator
	 */
	default <M> ExceptionIterator<M, E> map(MapExceptionIterator.MapWithIdFunction<T, M, E> mappingFunc) {
		return new MapExceptionIterator<>(this, mappingFunc);
	}

	/**
	 * map this iterator with a function
	 *
	 * @param mappingFunc the mapping function
	 * @param <M>         the new iterator type
	 * @return iterator
	 */
	default <M> ExceptionIterator<M, E> mapFiltered(FilterMapExceptionIterator.ExceptionFunction<T, M, E> mappingFunc) {
		return new FilterMapExceptionIterator<>(this, mappingFunc);
	}

	/**
	 * map this iterator with a function
	 *
	 * @param mappingFunc the mapping function
	 * @param <M>         the new iterator type
	 * @return iterator
	 */
	default <M> ExceptionIterator<M, E> mapFiltered(FilterMapExceptionIterator.MapWithIdFunction<T, M, E> mappingFunc) {
		return new FilterMapExceptionIterator<>(this, mappingFunc);
	}

	/**
	 * Convert to notification iterator
	 *
	 * @param estimatedSize the estimated size
	 * @param maxSplit      the maximum split
	 * @param message       message of the notification
	 * @param listener      listener
	 * @return notification iterator
	 */
	default ExceptionIterator<T, E> notif(long estimatedSize, long maxSplit, String message,
			ProgressListener listener) {
		return new NotificationExceptionIterator<>(this, estimatedSize, Math.max(maxSplit, estimatedSize / 10_000),
				message, listener);
	}

	/**
	 * convert this exception iterator to a base iterator and convert the
	 * exception to RuntimeException
	 *
	 * @return iterator
	 */
	default Iterator<T> asIterator() {
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				try {
					return ExceptionIterator.this.hasNext();
				} catch (Exception e) {
					if (e instanceof RuntimeException) {
						throw (RuntimeException) e;
					}
					throw new RuntimeException(e);
				}
			}

			@Override
			public T next() {
				try {
					return ExceptionIterator.this.next();
				} catch (Exception e) {
					if (e instanceof RuntimeException) {
						throw (RuntimeException) e;
					}
					throw new RuntimeException(e);
				}
			}

			@Override
			public void forEachRemaining(Consumer<? super T> action) {
				try {
					ExceptionIterator.this.forEachRemaining(action::accept);
				} catch (Exception e) {
					if (e instanceof RuntimeException) {
						throw (RuntimeException) e;
					}
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {
				try {
					ExceptionIterator.this.remove();
				} catch (Exception e) {
					if (e instanceof RuntimeException) {
						throw (RuntimeException) e;
					}
					throw new RuntimeException(e);
				}
			}
		};
	}

	/**
	 * @return -1 if undefined, the size of the iterator otherwise
	 */
	default long getSize() {
		return -1;
	}
}

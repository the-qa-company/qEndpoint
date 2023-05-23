package com.the_qa_company.qendpoint.core.storage.iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * {@link CloseableIterator} wrapper to attach closeables to a closeable
 * iterator
 *
 * @param <T>
 * @param <E>
 */
public class CloseableAttachIterator<T, E extends Exception> implements CloseableIterator<T, E> {
	@SafeVarargs
	public static <T, E extends Exception> CloseableIterator<T, E> of(CloseableIterator<T, E> it,
			AutoCloseableGeneric<E>... closeables) {
		if (closeables.length == 0) {
			return it;
		}
		return new CloseableAttachIterator<>(it, closeables);
	}

	private final CloseableIterator<T, E> handle;
	private final List<AutoCloseableGeneric<E>> closeables;

	@SafeVarargs
	private CloseableAttachIterator(CloseableIterator<T, E> handle, AutoCloseableGeneric<E>... closeableGenerics) {
		this.handle = handle;
		closeables = new ArrayList<>(List.of(closeableGenerics));
	}

	@Override
	public void close() throws E {
		try {
			handle.close();
		} catch (Error | Exception t) {
			try {
				AutoCloseableGeneric.<E>closeAll(closeables);
			} catch (RuntimeException | Error err) {
				err.addSuppressed(t);
				throw err;
			} catch (Exception ee) {
				t.addSuppressed(t);
			}
			throw t;
		}
	}

	@Override
	public boolean hasNext() {
		return handle.hasNext();
	}

	@Override
	public T next() {
		return handle.next();
	}

	@Override
	public void remove() {
		handle.remove();
	}

	@Override
	public CloseableIterator<T, E> attach(AutoCloseableGeneric<E> closeable) {
		closeables.add(closeable);
		return this;
	}

	@Override
	public void forEachRemaining(Consumer<? super T> action) {
		handle.forEachRemaining(action);
	}
}

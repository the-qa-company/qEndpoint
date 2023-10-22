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
public class CloseableAttachIterator<T extends RuntimeException> implements CloseableIterator<T> {
	@SafeVarargs
	public static <T> CloseableIterator<T> of(CloseableIterator<T> it,
                                                                   AutoCloseableGeneric<? extends RuntimeException>... closeables) {
		if (closeables.length == 0) {
			return it;
		}
		return new CloseableAttachIterator(it, closeables);
	}

	private final CloseableIterator<T> handle;
	private final List<AutoCloseableGeneric<RuntimeException>> closeables;

	@SafeVarargs
	private CloseableAttachIterator(CloseableIterator<T> handle, AutoCloseableGeneric<RuntimeException>... closeableGenerics) {
		this.handle = handle;
		closeables = new ArrayList<>(List.of(closeableGenerics));
	}

	@Override
	public void close()  {
		try {
			handle.close();
		} catch (Error | Exception t) {
			try {
				AutoCloseableGeneric.closeAll(closeables);
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
	public void forEachRemaining(Consumer<? super T> action) {
		handle.forEachRemaining(action);
	}
}

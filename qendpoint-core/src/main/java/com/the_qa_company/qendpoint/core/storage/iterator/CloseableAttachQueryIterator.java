package com.the_qa_company.qendpoint.core.storage.iterator;

import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class CloseableAttachQueryIterator implements QueryCloseableIterator {

	@SafeVarargs
	public static QueryCloseableIterator of(QueryCloseableIterator it,
			AutoCloseableGeneric<QEPCoreException>... closeables) {
		Objects.requireNonNull(it, "it can't be null!");
		if (closeables.length == 0) {
			return it;
		}
		return new CloseableAttachQueryIterator(it, closeables);
	}

	private final QueryCloseableIterator handle;
	private final List<AutoCloseableGeneric<QEPCoreException>> closeables;

	@SafeVarargs
	private CloseableAttachQueryIterator(QueryCloseableIterator handle,
			AutoCloseableGeneric<QEPCoreException>... closeableGenerics) {
		this.handle = handle;
		closeables = new ArrayList<>(List.of(closeableGenerics));
	}

	@Override
	public void close() throws QEPCoreException {
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
	public QEPComponentTriple next() {
		return handle.next();
	}

	@Override
	public void remove() {
		handle.remove();
	}

	@Override
	public long estimateCardinality() {
		return handle.estimateCardinality();
	}

	@Override
	public long lastId() {
		return handle.lastId();
	}

	@Override
	public QueryCloseableIterator attach(AutoCloseableGeneric<QEPCoreException> closeable) {
		closeables.add(closeable);
		return this;
	}

	@Override
	public void forEachRemaining(Consumer<? super QEPComponentTriple> action) {
		handle.forEachRemaining(action);
	}
}

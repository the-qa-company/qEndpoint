package com.the_qa_company.qendpoint.core.storage.iterator;

import java.util.Iterator;

public interface CloseableIterator<T, E extends Exception> extends Iterator<T>, AutoCloseable {
	@Override
	void close() throws E;
}

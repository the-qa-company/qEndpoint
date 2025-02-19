package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.concurrent.CompletableFuture;

public /**
		 * The asynchronous iterator interface. Its nextFuture() returns a
		 * CompletableFuture that completes with null when the iterator is
		 * exhausted or an exception if something goes wrong.
		 */
interface AsyncExceptionIterator<T, E extends Exception> {
	CompletableFuture<T> nextFuture();
}

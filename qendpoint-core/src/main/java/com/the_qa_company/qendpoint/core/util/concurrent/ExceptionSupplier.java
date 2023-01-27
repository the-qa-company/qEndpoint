package com.the_qa_company.qendpoint.core.util.concurrent;

@FunctionalInterface
public interface ExceptionSupplier<T, E extends Throwable> {
	T get() throws E;
}

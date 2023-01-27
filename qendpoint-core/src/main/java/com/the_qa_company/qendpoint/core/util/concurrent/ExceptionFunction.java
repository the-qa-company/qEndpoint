package com.the_qa_company.qendpoint.core.util.concurrent;

@FunctionalInterface
public interface ExceptionFunction<I, O, E extends Throwable> {
	O apply(I value) throws E;
}

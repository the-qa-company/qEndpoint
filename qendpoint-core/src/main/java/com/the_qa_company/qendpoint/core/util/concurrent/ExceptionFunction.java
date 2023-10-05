package com.the_qa_company.qendpoint.core.util.concurrent;

import java.io.IOException;
import java.io.InputStream;

@FunctionalInterface
public interface ExceptionFunction<I, O, E extends Throwable> {
	static <I, E extends Throwable> ExceptionFunction<I, I, E> identity() {
		return (a) -> a;
	}

	O apply(I value) throws E;
}

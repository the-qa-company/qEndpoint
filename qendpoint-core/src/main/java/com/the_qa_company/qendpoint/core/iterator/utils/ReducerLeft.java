package com.the_qa_company.qendpoint.core.iterator.utils;

public final class ReducerLeft<T> implements Reducer<T> {

	@Override
	public T reduce(T a, T b) {
		if (a.equals(b)) {
			return a;
		}
		return null;
	}
}

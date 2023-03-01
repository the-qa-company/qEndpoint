package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;

public class EmptyIterator<T> implements Iterator<T> {
	public static <T> Iterator<T> of() {
		return new EmptyIterator<>();
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public T next() {
		return null;
	}

}

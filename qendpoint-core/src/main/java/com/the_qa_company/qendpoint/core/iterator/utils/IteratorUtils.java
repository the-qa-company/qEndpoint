package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Iterator;

public final class IteratorUtils {
	public static <T> void printHead(Iterator<T> it, int count) {
		for (int i = 0; i < count; i++) {
			if (!it.hasNext())
				return;
			System.out.printf("%d -> %s\n", i, it.next());
		}
	}

	private IteratorUtils() {
	}
}

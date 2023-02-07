package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.*;

public class MergeExceptionIteratorTest {

	@Test
	public void mergeTest() {
		ExceptionIterator<String, RuntimeException> it1 = ExceptionIterator
				.of(Arrays.asList("1", "3", "5", "7").iterator());
		ExceptionIterator<String, RuntimeException> it2 = ExceptionIterator
				.of(Arrays.asList("2", "4", "6", "6").iterator());

		ExceptionIterator<String, RuntimeException> it = MergeExceptionIterator.buildOfTree(Function.identity(),
				String::compareTo, List.of(it1, it2), 0, 2);

		ExceptionIterator<String, RuntimeException> itExcepted = ExceptionIterator
				.of(Arrays.asList("1", "2", "3", "4", "5", "6", "6", "7").iterator());

		while (itExcepted.hasNext()) {
			assertTrue(it.hasNext());
			assertEquals(itExcepted.next(), it.next());
		}
		assertFalse(it.hasNext());
	}
}

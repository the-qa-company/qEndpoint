package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MergeExceptionIteratorStrategyTest {
	private static final String STRATEGY_PROPERTY = MergeExceptionIterator.MERGE_STRATEGY_SYSTEM_PROPERTY;

	@Test
	public void usesBinaryMergeIteratorWhenConfigured() throws Exception {
		String previous = System.getProperty(STRATEGY_PROPERTY);
		try {
			System.setProperty(STRATEGY_PROPERTY, "merge");
			ExceptionIterator<Integer, RuntimeException> merged = MergeExceptionIterator
					.buildOfTree(List.of(ExceptionIterator.of(List.of(1, 3, 5).iterator()),
							ExceptionIterator.of(List.of(2, 4, 6).iterator())), Integer::compareTo);

			assertTrue("Expected binary MergeExceptionIterator when merge strategy configured",
					merged instanceof MergeExceptionIterator);
			assertEquals(Integer.valueOf(1), merged.next());
			assertEquals(Integer.valueOf(2), merged.next());
			assertEquals(Integer.valueOf(3), merged.next());
		} finally {
			if (previous == null) {
				System.clearProperty(STRATEGY_PROPERTY);
			} else {
				System.setProperty(STRATEGY_PROPERTY, previous);
			}
		}
	}
}

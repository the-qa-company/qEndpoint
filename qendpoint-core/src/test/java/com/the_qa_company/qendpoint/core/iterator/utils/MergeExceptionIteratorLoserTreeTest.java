package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MergeExceptionIteratorLoserTreeTest {

	@Test
	public void buildOfTreePreservesSourceOrderOnEqualKeys() throws Exception {
		List<ExceptionIterator<RankedValue, RuntimeException>> sources = List.of(
				ExceptionIterator.of(List.of(new RankedValue(1, 0)).iterator()),
				ExceptionIterator.of(List.of(new RankedValue(1, 1)).iterator()),
				ExceptionIterator.of(List.of(new RankedValue(1, 2)).iterator()));

		ExceptionIterator<RankedValue, RuntimeException> merged = MergeExceptionIterator.buildOfTree(sources,
				(a, b) -> Integer.compare(a.key, b.key));

		List<Integer> order = new ArrayList<>();
		while (merged.hasNext()) {
			order.add(merged.next().source);
		}

		assertEquals(Arrays.asList(0, 1, 2), order);
	}

	private static final class RankedValue {
		private final int key;
		private final int source;

		private RankedValue(int key, int source) {
			this.key = key;
			this.source = source;
		}
	}
}

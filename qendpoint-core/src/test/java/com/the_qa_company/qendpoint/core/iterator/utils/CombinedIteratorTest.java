package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CombinedIteratorTest {
	@Test
	public void combineTest() {
		List<Iterator<Integer>> its = IntStream.range(0, 10)
				.mapToObj(l -> IntStream.range(l * 100, (l + 1) * 100).boxed().collect(Collectors.toList()).iterator())
				.collect(Collectors.toList());

		Iterator<Integer> it = CombinedIterator.combine(its);
		IntStream.range(0, 100 * 10).forEach(i -> {
			assertTrue(it.hasNext());
			assertEquals(i, (int) it.next());
		});
	}

}

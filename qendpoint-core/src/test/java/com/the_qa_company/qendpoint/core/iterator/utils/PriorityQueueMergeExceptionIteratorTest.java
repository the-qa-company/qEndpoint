package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PriorityQueueMergeExceptionIteratorTest {

	@Test
	public void mergeTest() throws Exception {
		ExceptionIterator<String, RuntimeException> it1 = ExceptionIterator
				.of(Arrays.asList("1", "3", "5", "7").iterator());
		ExceptionIterator<String, RuntimeException> it2 = ExceptionIterator
				.of(Arrays.asList("2", "4", "6", "6").iterator());

		ExceptionIterator<String, RuntimeException> it = LoserTreeMergeExceptionIterator.merge(List.of(it1, it2),
				String::compareTo);

		ExceptionIterator<String, RuntimeException> itExpected = ExceptionIterator
				.of(Arrays.asList("1", "2", "3", "4", "5", "6", "6", "7").iterator());

		while (itExpected.hasNext()) {
			assertTrue(it.hasNext());
			assertEquals(itExpected.next(), it.next());
		}
		assertFalse(it.hasNext());
	}

	@Test
	public void doesNotAdvanceBeforeReturningElement() throws Exception {
		ExceptionIterator<MutableInt, RuntimeException> it1 = new ReusingMutableIntIterator(1, 3, 5);
		ExceptionIterator<MutableInt, RuntimeException> it2 = new ReusingMutableIntIterator(2, 4, 6);

		ExceptionIterator<MutableInt, RuntimeException> merged = LoserTreeMergeExceptionIterator
				.merge(List.of(it1, it2), (a, b) -> Integer.compare(a.value, b.value));

		List<Integer> values = new ArrayList<>();
		while (merged.hasNext()) {
			MutableInt v = merged.next();
			values.add(v.value);
		}

		assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), values);
	}

	@Test
	public void mergeKeepsGoingWhenWinnerExhausts() throws Exception {
		ExceptionIterator<Integer, RuntimeException> it1 = ExceptionIterator.of(List.of(1).iterator());
		ExceptionIterator<Integer, RuntimeException> it2 = ExceptionIterator.of(List.of(2, 4, 6).iterator());
		ExceptionIterator<Integer, RuntimeException> it3 = ExceptionIterator.of(List.of(3, 5, 7).iterator());

		ExceptionIterator<Integer, RuntimeException> merged = LoserTreeMergeExceptionIterator
				.merge(List.of(it1, it2, it3), Integer::compareTo);

		List<Integer> out = new ArrayList<>();
		while (merged.hasNext()) {
			out.add(merged.next());
		}

		assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7), out);
	}

	@Test
	public void mergeRandomizedMatchesSortedConcatenation() throws Exception {
		Random random = new Random(42);

		for (int round = 0; round < 200; round++) {
			int nIterators = 1 + random.nextInt(20);
			List<ExceptionIterator<Integer, RuntimeException>> sources = new ArrayList<>(nIterators);
			List<Integer> expected = new ArrayList<>();

			for (int i = 0; i < nIterators; i++) {
				int size = random.nextInt(50);
				List<Integer> values = new ArrayList<>(size);
				int cur = random.nextInt(10);
				for (int j = 0; j < size; j++) {
					cur += random.nextInt(5);
					values.add(cur);
				}
				// Add duplicates sometimes.
				if (!values.isEmpty() && random.nextBoolean()) {
					values.add(values.get(values.size() - 1));
				}

				Collections.sort(values);
				expected.addAll(values);
				sources.add(ExceptionIterator.of(values.iterator()));
			}

			Collections.sort(expected);

			ExceptionIterator<Integer, RuntimeException> merged = LoserTreeMergeExceptionIterator.merge(sources,
					Integer::compareTo);
			List<Integer> actual = new ArrayList<>(expected.size());
			while (merged.hasNext()) {
				actual.add(merged.next());
			}

			assertEquals(expected, actual);
		}
	}

	private static final class MutableInt {
		int value;
	}

	private static final class ReusingMutableIntIterator implements ExceptionIterator<MutableInt, RuntimeException> {
		private final int[] values;
		private int index;
		private final MutableInt mutable = new MutableInt();

		private ReusingMutableIntIterator(int... values) {
			this.values = values;
		}

		@Override
		public boolean hasNext() {
			return index < values.length;
		}

		@Override
		public MutableInt next() {
			mutable.value = values[index++];
			return mutable;
		}
	}
}

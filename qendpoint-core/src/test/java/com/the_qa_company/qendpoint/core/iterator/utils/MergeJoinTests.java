package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Tests for the MergeJoinZipper and related asynchronous merge join iterators,
 * using JUnit 4.
 */
public class MergeJoinTests {

	// --- Helper iterator classes for testing ---

	/**
	 * A simple test iterator that wraps a List<T> and implements
	 * ExceptionIterator.
	 */
	public static class TestListIterator<T> implements ExceptionIterator<T, Exception> {
		private final List<T> list;
		private int index = 0;

		public TestListIterator(List<T> list) {
			this.list = list;
		}

		@Override
		public boolean hasNext() {
			return index < list.size();
		}

		@Override
		public T next() {
			if (!hasNext()) {
				throw new NoSuchElementException("Iterator exhausted");
			}
			return list.get(index++);
		}
	}

	/**
	 * A helper iterator that throws an exception at a specified index.
	 */
	public static class FailingTestListIterator<T> implements ExceptionIterator<T, Exception> {
		private final List<T> list;
		private final int failAt;
		private int index = 0;

		public FailingTestListIterator(List<T> list, int failAt) {
			this.list = list;
			this.failAt = failAt;
		}

		@Override
		public boolean hasNext() {
			return index < list.size();
		}

		@Override
		public T next() {
			if (index == failAt) {
				throw new RuntimeException("Intentional failure at index " + failAt);
			}
			if (!hasNext()) {
				throw new NoSuchElementException("Iterator exhausted");
			}
			return list.get(index++);
		}
	}

	/**
	 * Helper method: consumes an ExceptionIterator and returns its elements as
	 * a List.
	 */
	private <T> List<T> consumeIterator(ExceptionIterator<T, Exception> iterator) throws Exception {
		List<T> result = new ArrayList<>();
		while (iterator.hasNext()) {
			result.add(iterator.next());
		}
		return result;
	}

	/**
	 * Helper to wrap a synchronous ExceptionIterator in an
	 * AsyncPreFetchIterator and then in an AsyncToSyncExceptionIterator.
	 */
	private <T> ExceptionIterator<T, Exception> wrapSync(ExceptionIterator<T, Exception> iter) {
		AsyncPreFetchIterator<T, Exception> async = new AsyncPreFetchIterator<>(iter);
		return new AsyncToSyncExceptionIterator<>(async);
	}

	// --- Tests using MergeJoinZipper ---

	@Test
	public void testMergeJoinZipper_SimpleTwoIterators() throws Exception {
		List<Integer> list1 = Arrays.asList(1, 3, 5);
		List<Integer> list2 = Arrays.asList(2, 4, 6);

		List<ExceptionIterator<Integer, Exception>> input = new ArrayList<>();
		input.add(new TestListIterator<>(list1));
		input.add(new TestListIterator<>(list2));

		ExceptionIterator<Integer, Exception> merged = MergeJoinZipper.buildMergeTree(input, Integer::compareTo);

		List<Integer> actual = consumeIterator(merged);
		List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6);
		assertEquals(expected, actual);
	}

	@Test
	public void testMergeJoinZipper_OneEmptyOneNonEmpty() throws Exception {
		List<Integer> nonEmpty = Arrays.asList(10, 20, 30);

		List<ExceptionIterator<Integer, Exception>> input = new ArrayList<>();
		input.add(new TestListIterator<>(Collections.emptyList()));
		input.add(new TestListIterator<>(nonEmpty));

		ExceptionIterator<Integer, Exception> merged = MergeJoinZipper.buildMergeTree(input, Integer::compareTo);

		List<Integer> actual = consumeIterator(merged);
		assertEquals(nonEmpty, actual);
	}

	@Test
	public void testMergeJoinZipper_MultipleIterators() throws Exception {
		List<Integer> list1 = Arrays.asList(1, 4, 8);
		List<Integer> list2 = Arrays.asList(2, 3, 9);
		List<Integer> list3 = Arrays.asList(5, 6, 7);

		List<ExceptionIterator<Integer, Exception>> input = Arrays.asList(new TestListIterator<>(list1),
				new TestListIterator<>(list2), new TestListIterator<>(list3));

		ExceptionIterator<Integer, Exception> merged = MergeJoinZipper.buildMergeTree(input, Integer::compareTo);

		List<Integer> actual = consumeIterator(merged);
		List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
		assertEquals(expected, actual);
	}

	@Test
	public void testMergeJoinZipper_EmptyList() {
		ExceptionIterator<Integer, Exception> merged = MergeJoinZipper.buildMergeTree(Collections.emptyList(),
				Integer::compareTo);
		try {
			assertFalse(merged.hasNext());
		} catch (Exception e) {
			fail("hasNext() should not throw exception on empty iterator.");
		}
		try {
			merged.next();
			fail("Expected NoSuchElementException for empty iterator.");
		} catch (NoSuchElementException e) {
			// expected
		} catch (Exception e) {
			fail("Expected NoSuchElementException, but got: " + e);
		}
	}

	@Test
	public void testMergeJoinZipper_SingleIterator() throws Exception {
		List<Integer> data = Arrays.asList(100, 200, 300);
		List<ExceptionIterator<Integer, Exception>> input = Collections.singletonList(new TestListIterator<>(data));

		ExceptionIterator<Integer, Exception> merged = MergeJoinZipper.buildMergeTree(input, Integer::compareTo);

		List<Integer> actual = consumeIterator(merged);
		assertEquals(data, actual);
	}

	@Test
	public void testMergeJoinZipper_ExceptionPropagation() throws Exception {
		List<Integer> okList = Arrays.asList(1, 2, 3);
		FailingTestListIterator<Integer> failingIter = new FailingTestListIterator<>(Arrays.asList(10, 11, 12), 1);

		List<ExceptionIterator<Integer, Exception>> input = new ArrayList<>();
		input.add(new TestListIterator<>(okList));
		input.add(failingIter);

		ExceptionIterator<Integer, Exception> merged = MergeJoinZipper.buildMergeTree(input, Integer::compareTo);

		assertEquals(1, (int) merged.next());
		assertEquals(2, (int) merged.next());
		assertEquals(3, (int) merged.next());
		try {
			merged.next();
			fail("Expected exception from failing iterator.");
		} catch (Exception ex) {
			assertTrue(ex.getMessage().contains("Intentional failure"));
		}
	}

	@Test(timeout = 5000)
	public void testMergeJoinZipper_Cancellation() {
		ExceptionIterator<Integer, Exception> slowIter = new ExceptionIterator<Integer, Exception>() {
			private int count = 0;

			@Override
			public boolean hasNext() {
				return count < 5;
			}

			@Override
			public Integer next() throws Exception {
				Thread.sleep(200);
				return count++;
			}
		};

		AsyncPreFetchIterator<Integer, Exception> asyncIterator = new AsyncPreFetchIterator<>(slowIter);
		asyncIterator.cancel();
		CompletableFuture<Integer> future = asyncIterator.nextFuture();
		try {
			Integer value = future.join();
			assertNull(value);
		} catch (CompletionException ce) {
			assertTrue(ce.getCause() instanceof CancellationException);
		} finally {
			asyncIterator.close();
		}
	}

	@Test
	public void testMergeJoinZipper_RepeatedHasNext() throws Exception {
		List<Integer> data = Arrays.asList(5, 10, 15);
		ExceptionIterator<Integer, Exception> iter = wrapSync(new TestListIterator<>(data));

		assertTrue(iter.hasNext());
		assertTrue(iter.hasNext());
		assertEquals(5, (int) iter.next());

		assertTrue(iter.hasNext());
		assertTrue(iter.hasNext());
		assertEquals(10, (int) iter.next());

		assertTrue(iter.hasNext());
		assertEquals(15, (int) iter.next());
		assertFalse(iter.hasNext());
	}

	@Test
	public void testMergeJoinZipper_NextAfterExhaustion() throws Exception {
		List<Integer> data = Arrays.asList(100);
		ExceptionIterator<Integer, Exception> iter = wrapSync(new TestListIterator<>(data));

		assertTrue(iter.hasNext());
		assertEquals(100, (int) iter.next());
		assertFalse(iter.hasNext());
		try {
			iter.next();
			fail("Expected NoSuchElementException when calling next() after exhaustion.");
		} catch (NoSuchElementException e) {
			// Expected
		}
		try {
			iter.next();
			fail("Expected NoSuchElementException on subsequent calls after exhaustion.");
		} catch (NoSuchElementException e) {
			// Expected
		}
	}

	@Test
	public void testMergeJoinZipper_MergeSingleElementIterators() throws Exception {
		ExceptionIterator<Integer, Exception> iter1 = wrapSync(new TestListIterator<>(Arrays.asList(7)));
		ExceptionIterator<Integer, Exception> iter2 = wrapSync(new TestListIterator<>(Arrays.asList(3)));

		AsyncPreFetchIterator<Integer, Exception> async1 = new AsyncPreFetchIterator<>(iter1);
		AsyncPreFetchIterator<Integer, Exception> async2 = new AsyncPreFetchIterator<>(iter2);
		ZipperAsyncIterator<Integer, Exception> zipper = new ZipperAsyncIterator<>(async1, async2, Integer::compareTo);
		ExceptionIterator<Integer, Exception> merged = new AsyncToSyncExceptionIterator<>(zipper);

		List<Integer> result = consumeIterator(merged);
		List<Integer> expected = Arrays.asList(3, 7);
		assertEquals(expected, result);

		async1.close();
		async2.close();
	}

	@Test
	public void testMergeJoinZipper_PartialConsumption() throws Exception {
		List<Integer> list1 = Arrays.asList(0, 10, 20);
		List<Integer> list2 = Arrays.asList(5, 15, 25);
		List<ExceptionIterator<Integer, Exception>> input = Arrays.asList(new TestListIterator<>(list1),
				new TestListIterator<>(list2));

		ExceptionIterator<Integer, Exception> merged = MergeJoinZipper.buildMergeTree(input, Integer::compareTo);

		// Consume just the first two elements, then stop.
		assertTrue(merged.hasNext());
		assertEquals(0, (int) merged.next());
		assertTrue(merged.hasNext());
		assertEquals(5, (int) merged.next());

		// No further consumption.
	}

	@Test
	public void testMergeJoinZipper_Random() throws Exception {
		// Repeat the random test 5 times
		for (int i = 0; i < 5; i++) {
			Random rnd = new Random();
			int numLists = 2 + rnd.nextInt(4); // 2 to 5 lists
			List<List<Integer>> allLists = new ArrayList<>();
			List<ExceptionIterator<Integer, Exception>> input = new ArrayList<>();

			for (int j = 0; j < numLists; j++) {
				int size = rnd.nextInt(10);
				List<Integer> list = rnd.ints(size, 0, 100).boxed().sorted().collect(Collectors.toList());
				allLists.add(list);
				input.add(new TestListIterator<>(list));
			}

			ExceptionIterator<Integer, Exception> merged = MergeJoinZipper.buildMergeTree(input, Integer::compareTo);

			List<Integer> actual = consumeIterator(merged);
			List<Integer> expected = allLists.stream().flatMap(List::stream).sorted().collect(Collectors.toList());
			assertEquals(expected, actual);
		}
	}
}

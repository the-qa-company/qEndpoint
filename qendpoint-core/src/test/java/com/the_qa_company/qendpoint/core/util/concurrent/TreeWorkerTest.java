package com.the_qa_company.qendpoint.core.util.concurrent;

import com.the_qa_company.qendpoint.core.util.BitUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MergeExceptionIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TreeWorkerTest {
	@Parameterized.Parameters(name = "test {0} worker(s) {1} way(s)")
	public static Collection<Object[]> params() {
		return Arrays.asList(new Object[] { 1, 1 }, new Object[] { 8, 1 }, new Object[] { 1, 4 },
				new Object[] { 8, 4 });
	}

	private static class SyncSupplierTest implements TreeWorker.TreeWorkerSupplier<Integer> {
		private final int max;
		private final long sleep;
		private int val;
		private boolean inUse = false;

		public SyncSupplierTest(int max, long sleep) {
			this.max = max;
			this.sleep = sleep;
		}

		@Override
		public Integer get() {
			synchronized (this) {
				assertFalse(inUse);
				inUse = true;
			}
			sleepOrThrow(sleep);
			synchronized (this) {
				assertTrue(inUse);
				inUse = false;
			}
			if (val == max) {
				return null;
			}
			return ++val;
		}
	}

	private static Integer sum(Integer[] array, int count) {
		int s = 0;
		for (int i = 0; i < count; i++) {
			s += array[i];
		}
		return s;
	}

	private static class CountCatTest implements TreeWorker.TreeWorkerCat<Integer> {
		int call = 0;

		@Override
		public Integer construct(Integer[] array, int count) {
			synchronized (this) {
				call++;
			}
			return sum(array, count);
		}
	}

	private static class CountComparator implements Comparator<Integer> {
		int call = 0;

		@Override
		public int compare(Integer o1, Integer o2) {
			synchronized (this) {
				call++;
			}
			return Integer.compare(o1, o2);
		}
	}

	private static class IntegerArrayList extends ArrayList<Integer> {}

	@Parameterized.Parameter
	public int workers;
	@Parameterized.Parameter(1)
	public int ways;

	@Test
	public void syncSupplierTest() throws InterruptedException, TreeWorker.TreeWorkerException {
		TreeWorker.TreeWorkerCat<Integer> cat = TreeWorkerTest::sum;
		int max = 10;
		TreeWorker.TreeWorkerSupplier<Integer> supplier = new SyncSupplierTest(max, 20L);

		TreeWorker<Integer, Integer> worker = new TreeWorker<>(cat, supplier, null, TreeWorker.TreeWorkerMap.identity(),
				Integer[]::new, workers, ways);
		worker.start();
		Integer result = worker.waitToComplete();
		assertTrue(worker.isCompleted());
		assertNotNull(result);
		assertEquals(max * (max + 1) / 2, result.intValue());
	}

	@Test(expected = TreeWorker.TreeWorkerException.class)
	public void noElementSupplierTest() throws TreeWorker.TreeWorkerException {
		TreeWorker.TreeWorkerCat<Integer> cat = TreeWorkerTest::sum;
		int max = 0;
		TreeWorker.TreeWorkerSupplier<Integer> supplier = new SyncSupplierTest(max, 20L);

		// should crash because the supplier won't return any value to merge
		new TreeWorker<>(cat, supplier, null, TreeWorker.TreeWorkerMap.identity(), Integer[]::new, workers, ways);
	}

	@Test
	public void oneElementSupplierTest() throws InterruptedException, TreeWorker.TreeWorkerException {
		TreeWorker.TreeWorkerCat<Integer> cat = TreeWorkerTest::sum;
		int max = 1;
		TreeWorker.TreeWorkerSupplier<Integer> supplier = new SyncSupplierTest(max, 20L);

		TreeWorker<Integer, Integer> worker = new TreeWorker<>(cat, supplier, null, TreeWorker.TreeWorkerMap.identity(),
				Integer[]::new, workers, ways);
		worker.start();
		Integer result = worker.waitToComplete();
		assertTrue(worker.isCompleted());
		assertNotNull(result);
		assertEquals(1, result.intValue());
	}

	@Test
	public void catExceptionTest() throws InterruptedException, TreeWorker.TreeWorkerException {
		final String error = "I like HDT";
		TreeWorker.TreeWorkerCat<Integer> cat = (a, b) -> { throw new RuntimeException(error); };
		int max = 1;
		TreeWorker.TreeWorkerSupplier<Integer> supplier = new SyncSupplierTest(max, 20L);

		TreeWorker<Integer, Integer> worker = new TreeWorker<>(cat, supplier, null, TreeWorker.TreeWorkerMap.identity(),
				Integer[]::new, workers, ways);
		worker.start();
		try {
			worker.waitToComplete();
		} catch (TreeWorker.TreeWorkerException e) {
			assertEquals(error, e.getCause().getMessage());
		}
		assertTrue(worker.isCompleted());
	}

	@Test
	public void countTest() throws InterruptedException, TreeWorker.TreeWorkerException {
		CountCatTest cat = new CountCatTest();
		int max = 1 << 5;
		TreeWorker.TreeWorkerSupplier<Integer> supplier = new SyncSupplierTest(max, 2L);

		TreeWorker<Integer, Integer> worker = new TreeWorker<>(cat, supplier, null, TreeWorker.TreeWorkerMap.identity(),
				Integer[]::new, workers, ways);
		worker.start();
		Integer result = worker.waitToComplete();
		assertTrue(worker.isCompleted());
		assertNotNull(result);
		assertEquals(max * (max + 1) / 2, result.intValue());
	}

	@Test
	public void countAscendTest() throws InterruptedException, TreeWorker.TreeWorkerException {
		CountCatTest cat = new CountCatTest();
		int max = 1 << 5 - 1;
		TreeWorker.TreeWorkerSupplier<Integer> supplier = new SyncSupplierTest(max, 2L);

		TreeWorker<Integer, Integer> worker = new TreeWorker<>(cat, supplier, null, TreeWorker.TreeWorkerMap.identity(),
				Integer[]::new, workers, ways);
		worker.start();
		Integer result = worker.waitToComplete();
		assertTrue(worker.isCompleted());
		assertNotNull(result);
		assertEquals(max * (max + 1) / 2, result.intValue());
	}

	@Test
	public void deleteTest() throws TreeWorker.TreeWorkerException, InterruptedException {
		int max = 10;
		Set<Integer> elements = new HashSet<>();
		TreeWorker.TreeWorkerCat<Integer> cat = (array, count) -> {
			synchronized (elements) {
				for (int i = 0; i < count; i++) {
					elements.remove(array[i] * max);
				}
				int next = sum(array, count);
				elements.add(next * max);
				return next;
			}
		};
		TreeWorker.TreeWorkerSupplier<Integer> supplier = new TreeWorker.TreeWorkerSupplier<>() {
			int value = 0;

			@Override
			public Integer get() {
				if (value == max) {
					return null;
				}
				int v = ++value;
				synchronized (elements) {
					elements.add(v * max);
				}
				return v;
			}
		};

		TreeWorker.TreeWorkerDelete<Integer> delete = elements::remove;

		TreeWorker<Integer, Integer> worker = new TreeWorker<>(cat, supplier, delete,
				TreeWorker.TreeWorkerMap.identity(), Integer[]::new, workers, ways);
		worker.start();
		Integer result = worker.waitToComplete();
		assertTrue(worker.isCompleted());
		assertNotNull(result);
		assertEquals(1, elements.size());
		assertEquals(result * max, elements.iterator().next().intValue());
		assertEquals(max * (max + 1) / 2, result.intValue());
	}

	@Test
	public void mergeSortTest() throws TreeWorker.TreeWorkerException, InterruptedException {
		Random rnd = new Random(42);
		int count = 20;
		int maxValue = Integer.MAX_VALUE / 4;
		List<Integer> values = new ArrayList<>();
		List<Integer> lst = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			int v = rnd.nextInt(maxValue);
			values.add(v);
			lst.add(v);
		}
		assertEquals(lst, values);
		List<Integer> sorted = lst.stream().map(i -> i * 3).sorted(Comparator.comparingInt(a -> a))
				.collect(Collectors.toList());
		assertNotEquals(sorted, values);
		CountComparator com = new CountComparator();
		assertTrue(com.compare(1325939940, -1360544799) > 0);
		assertTrue(com.compare(2, 1) > 0);
		assertTrue(com.compare(-3, -2) < 0);
		assertTrue(com.compare(-2, -3) > 0);
		com.call = 0;
		TreeWorker<IntegerArrayList, IntegerArrayList> worker = new TreeWorker<>(
				(IntegerArrayList[] array, int length) -> {
					Iterator<Integer> it = MergeExceptionIterator
							.buildOfTree(l -> ExceptionIterator.of(l.iterator()), com, array, length).asIterator();
					IntegerArrayList l = new IntegerArrayList();
					while (it.hasNext()) {
						l.add(it.next());
					}
					IntegerArrayList tst = new IntegerArrayList();
					tst.addAll(l);
					tst.sort(Integer::compareTo);
					sleepOrThrow(25);
					assertEquals(tst, l);
					return l;
				}, new TreeWorker.TreeWorkerSupplier<>() {
					int index;

					@Override
					public IntegerArrayList get() {
						if (index == values.size()) {
							return null;
						}
						IntegerArrayList l = new IntegerArrayList();
						l.add(values.get(index++));
						sleepOrThrow(25);
						return l;
					}
				}, null, v -> v.stream().map(i -> i * 3).collect(Collectors.toCollection(IntegerArrayList::new)),
				IntegerArrayList[]::new, workers, ways);
		worker.start();
		List<Integer> result = worker.waitToComplete();
		// test O(n log(n))
		assertTrue("calls: " + com.call + ", n logn : " + count * BitUtil.log2(count),
				com.call <= count * BitUtil.log2(count));
		assertEquals(sorted, result);
	}

	private static void sleepOrThrow(long time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			throw new AssertionError("Interruption", e);
		}
	}
}

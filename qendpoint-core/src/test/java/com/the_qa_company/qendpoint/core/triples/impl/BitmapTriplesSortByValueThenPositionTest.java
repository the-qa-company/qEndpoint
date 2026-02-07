package com.the_qa_company.qendpoint.core.triples.impl;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.concurrent.locks.StampedLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class BitmapTriplesSortByValueThenPositionTest {
	private static final class Pair {
		private final long value;
		private final long position;

		private Pair(long value, long position) {
			this.value = value;
			this.position = position;
		}
	}

	@Test
	public void sortByValueThenPositionOrdersPairs() throws Exception {
		long[] values = { 5L, 2L, 2L, 1L, 5L, 3L };
		long[] positions = { 9L, 4L, 1L, 7L, 3L, 8L };

		Pair[] expected = new Pair[values.length];
		for (int i = 0; i < values.length; i++) {
			expected[i] = new Pair(values[i], positions[i]);
		}
		Arrays.sort(expected,
				Comparator.comparingLong((Pair pair) -> pair.value).thenComparingLong(pair -> pair.position));

		Method method = BitmapTriples.class.getDeclaredMethod("sortByValueThenPosition", long[].class, long[].class,
				int.class, int.class);
		method.setAccessible(true);
		method.invoke(null, values, positions, 0, values.length);

		for (int i = 0; i < values.length; i++) {
			assertEquals(expected[i].value, values[i]);
			assertEquals(expected[i].position, positions[i]);
		}
	}

	@Test
	public void timSortMergesLongRuns() throws Exception {
		int run = 1024;
		int length = run * 2;
		long[] values = new long[length];
		long[] positions = new long[length];

		for (int i = 0; i < run; i++) {
			values[i] = i;
			positions[i] = i;
		}
		for (int i = 0; i < run; i++) {
			values[run + i] = i;
			positions[run + i] = run + i;
		}

		Pair[] expected = new Pair[length];
		for (int i = 0; i < length; i++) {
			expected[i] = new Pair(values[i], positions[i]);
		}
		Arrays.sort(expected,
				Comparator.comparingLong((Pair pair) -> pair.value).thenComparingLong(pair -> pair.position));

		Class<?> timSortClass = Class
				.forName("com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples$LongPairTimSort");
		Method sort = timSortClass.getDeclaredMethod("sort", long[].class, long[].class, int.class, int.class);
		sort.setAccessible(true);
		sort.invoke(null, values, positions, 0, length);

		for (int i = 0; i < length; i++) {
			assertEquals(expected[i].value, values[i]);
			assertEquals(expected[i].position, positions[i]);
		}
	}

	@Test
	public void parallelQuickSortOrdersPairs() throws Exception {
		int length = 1 << 18;
		long[] values = new long[length];
		long[] positions = new long[length];

		long seed = 1L;
		for (int i = 0; i < length; i++) {
			seed = seed * 6364136223846793005L + 1;
			values[i] = seed & 1023L;
			positions[i] = i;
		}

		Pair[] expected = new Pair[length];
		for (int i = 0; i < length; i++) {
			expected[i] = new Pair(values[i], positions[i]);
		}
		Arrays.sort(expected,
				Comparator.comparingLong((Pair pair) -> pair.value).thenComparingLong(pair -> pair.position));

		Method sort = BitmapTriples.class.getDeclaredMethod("parallelQuickSortPairs", long[].class, long[].class,
				int.class, int.class);
		sort.setAccessible(true);
		sort.invoke(null, values, positions, 0, length - 1);

		for (int i = 0; i < length; i++) {
			assertEquals(expected[i].value, values[i]);
			assertEquals(expected[i].position, positions[i]);
		}
	}

	@Test
	public void estimateAvailableMemoryCachesBetweenRefreshes() throws Exception {
		Method method = BitmapTriples.class.getDeclaredMethod("estimateAvailableMemory");
		method.setAccessible(true);

		java.lang.reflect.Field cacheField = BitmapTriples.class.getDeclaredField("MEMORY_ESTIMATE_CACHE");
		java.lang.reflect.Field counterField = BitmapTriples.class.getDeclaredField("MEMORY_ESTIMATE_CALLS");
		cacheField.setAccessible(true);
		counterField.setAccessible(true);

		long originalCache = cacheField.getLong(null);
		int originalCounter = counterField.getInt(null);

		try {
			long sentinel = Long.MAX_VALUE / 4;
			cacheField.setLong(null, sentinel);
			counterField.setInt(null, 0);

			long cached = ((Long) method.invoke(null)).longValue();
			assertEquals(sentinel, cached);

			cacheField.setLong(null, sentinel);
			counterField.setInt(null, 99);

			long refreshed = ((Long) method.invoke(null)).longValue();
			assertNotEquals(sentinel, refreshed);
		} finally {
			cacheField.setLong(null, originalCache);
			counterField.setInt(null, originalCounter);
		}
	}

	@Test
	public void longArrayPoolReusesArrays() throws Exception {
		resetLongArrayPool();
		Class<?> poolClass = Class.forName("com.the_qa_company.qendpoint.core.triples.impl.LongArrayPool");
		Method borrow = poolClass.getDeclaredMethod("borrow", int.class);
		Method release = poolClass.getDeclaredMethod("release", long[].class);
		borrow.setAccessible(true);
		release.setAccessible(true);

		long[] first = (long[]) borrow.invoke(null, 8);
		release.invoke(null, new Object[] { first });

		long[] second = (long[]) borrow.invoke(null, 8);
		assertSame(first, second);
	}

	@Test
	public void longArrayPoolSkipsTinyArrays() throws Exception {
		resetLongArrayPool();
		Class<?> poolClass = Class.forName("com.the_qa_company.qendpoint.core.triples.impl.LongArrayPool");
		Method borrow = poolClass.getDeclaredMethod("borrow", int.class);
		Method release = poolClass.getDeclaredMethod("release", long[].class);
		borrow.setAccessible(true);
		release.setAccessible(true);

		long[] first = (long[]) borrow.invoke(null, 3);
		release.invoke(null, new Object[] { first });

		long[] second = (long[]) borrow.invoke(null, 3);
		assertNotSame(first, second);
	}

	@Test
	public void longArrayPoolRetainsSmallStacks() throws Exception {
		resetLongArrayPool();
		Class<?> poolClass = Class.forName("com.the_qa_company.qendpoint.core.triples.impl.LongArrayPool");
		Method setConcurrency = poolClass.getDeclaredMethod("setConcurrency", int.class);
		Method borrow = poolClass.getDeclaredMethod("borrow", int.class);
		Method release = poolClass.getDeclaredMethod("release", long[].class);
		Field poolGroupField = poolClass.getDeclaredField("poolGroup");
		setConcurrency.setAccessible(true);
		borrow.setAccessible(true);
		release.setAccessible(true);
		poolGroupField.setAccessible(true);

		Object originalPoolGroup = poolGroupField.get(null);
		Field concurrencyField = originalPoolGroup.getClass().getDeclaredField("concurrency");
		concurrencyField.setAccessible(true);
		int originalConcurrency = concurrencyField.getInt(originalPoolGroup);

		setConcurrency.invoke(null, 1);
		try {
			long[] first = (long[]) borrow.invoke(null, 8);
			release.invoke(null, new Object[] { first });

			long[] second = (long[]) borrow.invoke(null, 8);
			assertSame(first, second);

			Object poolGroup = poolGroupField.get(null);
			Field poolsField = poolGroup.getClass().getDeclaredField("pools");
			poolsField.setAccessible(true);
			Object[] pools = (Object[]) poolsField.get(poolGroup);
			Object subPool = pools[0];

			Field poolField = subPool.getClass().getDeclaredField("pool");
			poolField.setAccessible(true);
			@SuppressWarnings("unchecked")
			NavigableMap<Integer, ?> pool = (NavigableMap<Integer, ?>) poolField.get(subPool);
			Object stack = pool.get(8);
			assertNotNull(stack);

			Field sizeField = stack.getClass().getDeclaredField("size");
			sizeField.setAccessible(true);
			assertEquals(0, sizeField.getInt(stack));
		} finally {
			setConcurrency.invoke(null, originalConcurrency);
		}
	}

	@Test
	public void longArrayPoolDisabledDoesNotReuseArrays() throws Exception {
		Class<?> poolClass = Class.forName("com.the_qa_company.qendpoint.core.triples.impl.LongArrayPool");
		Method setEnabled = poolClass.getDeclaredMethod("setEnabled", boolean.class);
		Method borrow = poolClass.getDeclaredMethod("borrow", int.class);
		Method release = poolClass.getDeclaredMethod("release", long[].class);
		setEnabled.setAccessible(true);
		borrow.setAccessible(true);
		release.setAccessible(true);

		setEnabled.invoke(null, false);
		try {
			long[] first = (long[]) borrow.invoke(null, 8);
			release.invoke(null, new Object[] { first });

			long[] second = (long[]) borrow.invoke(null, 8);
			assertNotSame(first, second);
		} finally {
			setEnabled.invoke(null, true);
		}
	}

	@Test
	public void longArrayPoolUsesStampedLock() throws Exception {
		Class<?> subPoolClass = Class.forName("com.the_qa_company.qendpoint.core.triples.impl.LongArrayPool$SubPool");
		Field lockField = subPoolClass.getDeclaredField("lock");
		lockField.setAccessible(true);

		assertEquals(StampedLock.class, lockField.getType());
	}

	@Test
	public void longArrayPoolShardsByThreadIdModulo() throws Exception {
		Class<?> poolClass = Class.forName("com.the_qa_company.qendpoint.core.triples.impl.LongArrayPool");
		Method setEnabled = poolClass.getDeclaredMethod("setEnabled", boolean.class);
		Method setConcurrency = poolClass.getDeclaredMethod("setConcurrency", int.class);
		Method borrow = poolClass.getDeclaredMethod("borrow", int.class);
		Method release = poolClass.getDeclaredMethod("release", long[].class);
		setEnabled.setAccessible(true);
		setConcurrency.setAccessible(true);
		borrow.setAccessible(true);
		release.setAccessible(true);

		setEnabled.invoke(null, false);
		setEnabled.invoke(null, true);
		setConcurrency.invoke(null, 2);

		ThreadResult first = borrowInThread(borrow, release);
		ThreadResult second = borrowInThread(borrow, release);
		int attempts = 0;
		while (first.threadId % 2 == second.threadId % 2 && attempts < 4) {
			second = borrowInThread(borrow, release);
			attempts++;
		}

		assertNotEquals(first.threadId % 2, second.threadId % 2);
		assertNotSame(first.array, second.array);
	}

	@Test
	public void timSortEnsureCapacityGrowsBothBuffers() throws Exception {
		Class<?> timSortClass = Class
				.forName("com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples$LongPairTimSort");
		java.lang.reflect.Constructor<?> ctor = timSortClass.getDeclaredConstructor(long[].class, long[].class,
				int.class);
		ctor.setAccessible(true);

		long[] values = new long[512];
		long[] positions = new long[512];
		Object sorter = ctor.newInstance(values, positions, 512);

		java.lang.reflect.Field tmpValuesField = timSortClass.getDeclaredField("tmpValues");
		java.lang.reflect.Field tmpPositionsField = timSortClass.getDeclaredField("tmpPositions");
		tmpValuesField.setAccessible(true);
		tmpPositionsField.setAccessible(true);
		tmpValuesField.set(sorter, new long[512]);
		tmpPositionsField.set(sorter, new long[256]);

		Method ensureCapacity = timSortClass.getDeclaredMethod("ensureCapacity", int.class);
		ensureCapacity.setAccessible(true);
		ensureCapacity.invoke(sorter, 352);

		long[] tmpPositions = (long[]) tmpPositionsField.get(sorter);
		assertTrue(tmpPositions.length >= 352);
	}

	private static ThreadResult borrowInThread(Method borrow, Method release) throws Exception {
		ThreadResult result = new ThreadResult();
		Throwable[] failure = new Throwable[1];
		Thread thread = new Thread(() -> {
			try {
				result.threadId = Thread.currentThread().getId();
				result.array = (long[]) borrow.invoke(null, 8);
				release.invoke(null, new Object[] { result.array });
			} catch (Throwable throwable) {
				failure[0] = throwable;
			}
		});
		thread.start();
		thread.join(5000L);
		if (thread.isAlive()) {
			thread.interrupt();
			throw new AssertionError("Worker thread did not finish in time");
		}
		if (failure[0] != null) {
			throw new AssertionError("Worker thread failed", failure[0]);
		}
		return result;
	}

	private static void resetLongArrayPool() throws Exception {
		Class<?> poolClass = Class.forName("com.the_qa_company.qendpoint.core.triples.impl.LongArrayPool");
		Method setEnabled = poolClass.getDeclaredMethod("setEnabled", boolean.class);
		setEnabled.setAccessible(true);
		setEnabled.invoke(null, false);
		setEnabled.invoke(null, true);
	}

	private static final class ThreadResult {
		private long threadId;
		private long[] array;
	}
}

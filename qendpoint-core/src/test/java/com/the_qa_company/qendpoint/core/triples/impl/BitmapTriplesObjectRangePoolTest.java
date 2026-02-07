package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64Big;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import static org.junit.Assert.assertEquals;

public class BitmapTriplesObjectRangePoolTest {
	@Test
	public void sortObjectRangeSequentialUsesRequestedLength() throws Exception {
		resetLongArrayPool();
		LongArrayPool.release(new long[8]);

		BitmapTriples triples = new BitmapTriples(new HDTSpecification());
		Sequence seqY = sequenceWithValues(32, new long[] { 40, 10, 30, 20 });
		DynamicSequence objectArray = sequenceWithValues(32, new long[] { 0, 1, 2, 3 });

		Method method = BitmapTriples.class.getDeclaredMethod("sortObjectRangeSequential", Sequence.class,
				DynamicSequence.class, long.class, long.class);
		method.setAccessible(true);
		method.invoke(triples, seqY, objectArray, 0L, 4L);

		assertEquals(1L, objectArray.get(0));
		assertEquals(3L, objectArray.get(1));
		assertEquals(2L, objectArray.get(2));
		assertEquals(0L, objectArray.get(3));
	}

	@Test
	public void sortObjectRangesBatchUsesRequestedLength() throws Exception {
		resetLongArrayPool();
		LongArrayPool.release(new long[8]);

		BitmapTriples triples = new BitmapTriples(new HDTSpecification());
		Sequence seqY = sequenceWithValues(32, new long[] { 40, 10, 30, 20 });
		DynamicSequence objectArray = sequenceWithValues(32, new long[] { 0, 1, 2, 3 });

		List<Object> ranges = new ArrayList<>();
		ranges.add(newObjectSortRange(0L, 4));

		Method method = BitmapTriples.class.getDeclaredMethod("sortObjectRangesBatch", ForkJoinPool.class, List.class,
				Sequence.class, DynamicSequence.class);
		method.setAccessible(true);
		ForkJoinPool pool = new ForkJoinPool(1);
		try {
			method.invoke(triples, pool, ranges, seqY, objectArray);
		} finally {
			pool.shutdown();
		}

		assertEquals(1L, objectArray.get(0));
		assertEquals(3L, objectArray.get(1));
		assertEquals(2L, objectArray.get(2));
		assertEquals(0L, objectArray.get(3));
	}

	private static DynamicSequence sequenceWithValues(int numbits, long[] values) {
		DynamicSequence sequence = new SequenceLog64Big(numbits, values.length, true);
		for (int i = 0; i < values.length; i++) {
			sequence.set(i, values[i]);
		}
		return sequence;
	}

	private static Object newObjectSortRange(long start, int length) throws Exception {
		Class<?> rangeClass = Class
				.forName("com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples$ObjectSortRange");
		Constructor<?> ctor = rangeClass.getDeclaredConstructor(long.class, int.class);
		ctor.setAccessible(true);
		return ctor.newInstance(start, length);
	}

	private static void resetLongArrayPool() throws Exception {
		LongArrayPool.setEnabled(false);
		LongArrayPool.setEnabled(true);
	}
}

package com.the_qa_company.qendpoint.core.util.disk;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class AbstractLongArrayTest {

	@Test
	public void updateEstimatedLocationArrayBucketSize() {

		Random random = new Random();
		AbstractLongArray longArray = new AbstractLongArray() {
			@Override
			public long get(long index) {
				return Math.max(0, maxValue - index);
			}

			@Override
			public long length() {
				return AbstractLongArray.ESTIMATED_LOCATION_ARRAY_SIZE * 1024L;
			}

			@Override
			public int sizeOf() {
				return 0;
			}

			@Override
			public void resize(long newSize) throws IOException {

			}

			@Override
			public void clear() {

			}

			@Override
			protected void innerSet(long index, long value) {

			}
		};

		longArray.recalculateEstimatedValueLocation();

		for (long i = 0; i < (AbstractLongArray.ESTIMATED_LOCATION_ARRAY_SIZE * 1024L) + 3; i++) {
			testMaxValue(longArray, i);
		}

		longArray.recalculateEstimatedValueLocation();

		for (long i = 1; i > 0 && i < (Long.MAX_VALUE - 1); i *= 2) {
			testMaxValue(longArray, i);
		}

		longArray.recalculateEstimatedValueLocation();

		testMaxValue(longArray, Long.MAX_VALUE);
		System.out.println();

		longArray.recalculateEstimatedValueLocation();

		long estimatedLocation = longArray.getEstimatedLocation(Long.MAX_VALUE, -1, Long.MAX_VALUE);
		long estimatedLocationLowerBound = longArray.getEstimatedLocationLowerBound(Long.MAX_VALUE);
		long estimatedLocationUpperBound = longArray.getEstimatedLocationUpperBound(Long.MAX_VALUE);
		System.out.println(estimatedLocation);
		System.out.println(estimatedLocationLowerBound);
		System.out.println(estimatedLocationUpperBound);
	}

	private static void testMaxValue(AbstractLongArray longArray, long value) {

		longArray.maxValue = value;

		longArray.updateEstimatedLocationArrayBucketSize();

		long estimatedLocationArrayBucketSize = longArray.getEstimatedLocationArrayBucketSize();

		Assert.assertTrue(estimatedLocationArrayBucketSize + "", estimatedLocationArrayBucketSize > 0);

		longArray.getEstimatedLocation(value, -1, Long.MAX_VALUE);
		longArray.getEstimatedLocationLowerBound(value);
		longArray.getEstimatedLocationUpperBound(value);
	}
}

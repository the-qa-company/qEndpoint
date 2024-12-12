package com.the_qa_company.qendpoint.core.util.disk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractLongArray implements LongArray {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private static final int ESTIMATED_LOCATION_ARRAY_SIZE;

	static {
		// get total amount of memory that this java program is allowed to use
		long maxMemory = Runtime.getRuntime().maxMemory();

		if (maxMemory >= 1024 * 1024 * 512) {
			ESTIMATED_LOCATION_ARRAY_SIZE = 1024 * 128;
		} else if (maxMemory >= 1024 * 1024 * 256) {
			ESTIMATED_LOCATION_ARRAY_SIZE = 1024 * 64;
		} else if (maxMemory >= 1024 * 1024 * 128) {
			ESTIMATED_LOCATION_ARRAY_SIZE = 1024 * 32;
		} else {
			ESTIMATED_LOCATION_ARRAY_SIZE = 1024 * 16;
		}

	}

	private final long[] estimatedLocationMax = new long[ESTIMATED_LOCATION_ARRAY_SIZE];
	private final long[] estimatedLocationMin = new long[ESTIMATED_LOCATION_ARRAY_SIZE];
	private final long[] estimatedLocation = new long[ESTIMATED_LOCATION_ARRAY_SIZE];

	private int estimatedLocationBucketSize;

	long maxValue = 1;

	@Override
	public int getEstimatedLocationArrayBucketSize() {
		return estimatedLocationBucketSize;
	}

	private void updateEstimatedLocationArrayBucketSize() {
		int minBucketSize = (int) (maxValue / ESTIMATED_LOCATION_ARRAY_SIZE);
		// we want to have the next power of 2
		int next = 1;
		while (next < minBucketSize) {
			next <<= 1;
		}
		this.estimatedLocationBucketSize = next;
	}

	@Override
	public long[] getEstimatedLocationArray() {
		return estimatedLocation;
	}

	@Override
	public long[] getEstimatedLocationArrayMin() {
		return estimatedLocationMin;
	}

	@Override
	public long[] getEstimatedLocationArrayMax() {
		return estimatedLocationMax;
	}

	@Override
	public void recalculateEstimatedValueLocation() {
		updateEstimatedLocationArrayBucketSize();
		int estimatedLocationBucketSize = getEstimatedLocationArrayBucketSize();
		long len = length();
		boolean shouldLog = len > 1024 * 1024 * 2;
		if (shouldLog) {
			logger.info("Recalculating estimated location array 0%");
		}

		for (int i = 0; i < len; i++) {
			long val = get(i);
			if (val == 0) {
				continue;
			}

			int index = (int) (val / estimatedLocationBucketSize + 1);
			estimatedLocationMax[index] = Math.max(estimatedLocationMax[index], i);
			if (estimatedLocationMin[index] == 0) {
				estimatedLocationMin[index] = i;
			} else {
				estimatedLocationMin[index] = Math.min(estimatedLocationMin[index], i);
			}
			estimatedLocation[index] = (estimatedLocationMax[index] + estimatedLocationMin[index]) / 2;

			if (shouldLog && i % (1024 * 1024) == 0) {
				logger.info("Recalculating estimated location array {}%", (int) Math.floor(100.0 / len * i));
			}
		}

		if (shouldLog) {
			logger.info("Recalculating estimated location array 100%");
		}
	}

	@Override
	public final void set(long index, long value) {
		maxValue = Math.max(maxValue, value);
		innerSet(index, value);
	}

	abstract protected void innerSet(long index, long value);

}

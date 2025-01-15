package com.the_qa_company.qendpoint.core.util.disk;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap375Big;
import com.the_qa_company.qendpoint.core.util.BitUtil;

public abstract class AbstractLongArray implements LongArray {

	static final int ESTIMATED_LOCATION_ARRAY_SIZE;

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

	private long estimatedLocationBucketSize = 1;

	long maxValue = 1;

	@Override
	public long getEstimatedLocationArrayBucketSize() {
		return estimatedLocationBucketSize;
	}

	void updateEstimatedLocationArrayBucketSize() {
		this.estimatedLocationBucketSize = maxValue / ESTIMATED_LOCATION_ARRAY_SIZE + 1;
//		this.estimatedLocationBucketSize = ((1L << BitUtil.log2(maxValue)) - 1) / ESTIMATED_LOCATION_ARRAY_SIZE + 1;
		assert this.estimatedLocationBucketSize > 0;
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
		if (Bitmap375Big.oldBinarySearch) {
			return;
		}

		updateEstimatedLocationArrayBucketSize();
		long estimatedLocationBucketSize = getEstimatedLocationArrayBucketSize();
		long len = length();

		for (long i = 0; i < len; i++) {
			long val = get(i);
			if (val == 0) {
				// val shouldn't be zero, since this represents a value that
				// does not exist
				continue;
			}

			int index = getEstimatedLocationIndex(val);
			if (index >= estimatedLocation.length || index >= estimatedLocationMax.length
					|| index >= estimatedLocationMin.length) {
				logger.warn("Index out of bounds for " + getClass().getSimpleName()
						+ " when recalculateEstimatedValueLocation for value " + val + " and estimatedBucketSize "
						+ estimatedLocationBucketSize + " and index " + index + " and estimatedLocation.length "
						+ estimatedLocation.length + " and estimatedLocationMax.length " + estimatedLocationMax.length
						+ " and estimatedLocationMin.length " + estimatedLocationMin.length);
				continue;
			}
			estimatedLocationMax[index] = Math.max(estimatedLocationMax[index], i);
			if (estimatedLocationMin[index] == 0) {
				estimatedLocationMin[index] = i;
			} else {
				estimatedLocationMin[index] = Math.min(estimatedLocationMin[index], i);
			}
			estimatedLocation[index] = (estimatedLocationMax[index] + estimatedLocationMin[index]) / 2;
		}
	}

	@Override
	public final void set(long index, long value) {
		maxValue = Math.max(maxValue, value);
		innerSet(index, value);
	}

	abstract protected void innerSet(long index, long value);

}

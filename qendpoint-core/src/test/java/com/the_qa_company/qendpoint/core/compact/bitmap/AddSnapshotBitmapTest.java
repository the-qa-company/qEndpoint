package com.the_qa_company.qendpoint.core.compact.bitmap;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AddSnapshotBitmapTest {
	/**
	 * create a bitmap copy of another bitmap
	 *
	 * @param other other bm
	 * @param size  size of the bitmap
	 * @return bitmap
	 */
	public static Bitmap createBitmapCopy(Bitmap other, long size) {
		ModifiableBitmap bm = Bitmap64Big.memory(size);
		for (long i = 0; i < size; i++) {
			bm.set(i, other.access(i));
		}
		return bm;
	}

	public static void assertBitmapEquals(Bitmap excepted, Bitmap actual, long size) {
		for (long i = 0; i < size; i++) {
			assertEquals("bits #" + i + " aren't the same", excepted.access(i), actual.access(i));
		}
	}

	public static void assertBitmapNotEquals(Bitmap excepted, Bitmap actual, long size) {
		for (long i = 0; i < size; i++) {
			if (excepted.access(i) != actual.access(i)) {
				return;
			}
		}
		fail("the 2 bitmaps are equal");
	}

	@Test
	public void snapshotTest() {
		final long size = 10_000;
		AddSnapshotBitmap asb = AddSnapshotBitmap.of(Bitmap64Big.memory(size));

		AddSnapshotBitmap.AddSnapshotDeltaBitmap s0 = asb.createSnapshot();
		Bitmap s0c = createBitmapCopy(asb, size);
		AddSnapshotBitmap.AddSnapshotDeltaBitmap s1 = asb.createSnapshot();
		Bitmap s1c = createBitmapCopy(asb, size);
		AddSnapshotBitmap.AddSnapshotDeltaBitmap s2 = asb.createSnapshot();
		Bitmap s2c = createBitmapCopy(asb, size);
		assertBitmapEquals(asb, s0, size);
		assertBitmapEquals(asb, s0c, size);
		assertBitmapEquals(s0c, s0, size);

		asb.set(12, true);
		assertBitmapEquals(s0c, s0, size);
		assertBitmapEquals(s1c, s1, size);
		assertBitmapEquals(s2c, s2, size);
		assertBitmapNotEquals(asb, s0, size);
		assertBitmapNotEquals(asb, s1, size);
		assertBitmapNotEquals(asb, s2, size);

		s2.close();
		assertBitmapEquals(s0c, s0, size);
		assertBitmapEquals(s1c, s1, size);

		AddSnapshotBitmap.AddSnapshotDeltaBitmap s3 = asb.createSnapshot();
		Bitmap s3c = createBitmapCopy(asb, size);
		assertBitmapEquals(asb, s3c, size);
		assertBitmapEquals(asb, s3, size);

		s0.close();
		assertBitmapEquals(s1c, s1, size);
		assertBitmapNotEquals(s1, s3, size);
	}
}

package com.the_qa_company.qendpoint.core.compact.bitmap;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NegBitmapTest {
	@Test
	public void negBitmapTest() {
		long size = 53;
		ModifiableBitmap bitmap = NegBitmap.of(BitmapFactory.createRWBitmap(size));

		for (long i = 0; i < size; i++) {
			assertTrue(bitmap.access(i));
		}

		bitmap.set(4, false);

		assertFalse(bitmap.access(4));

		bitmap.set(4, true);

		assertTrue(bitmap.access(4));
	}
}

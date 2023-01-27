package com.the_qa_company.qendpoint.core.compact.bitmap;

import org.junit.Assert;
import org.junit.Test;

public class BitmapFactoryTest {

	@Test
	public void emptyBitmapTest() {
		final long size = 123457;
		Bitmap bitmap = BitmapFactory.createRWBitmap(size);
		Assert.assertEquals("countOnes", bitmap.countOnes(), 0);

		// because we can't get size, we need to use the closest 64 multiple
		long realSize = (size & ~63) + ((size & 63) == 0 ? 0 : 64);

		Assert.assertEquals("countZeros", realSize, bitmap.countZeros());
	}
}

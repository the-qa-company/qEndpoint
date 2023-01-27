package com.the_qa_company.qendpoint.core.compact.sequence;

import org.junit.Test;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.visnow.jlargearrays.LargeArray;

public class LargeArrayTest {

	@Test
	public void allocationTest() {
		int old = LargeArray.getMaxSizeOf32bitArray();
		try {
			LargeArray.setMaxSizeOf32bitArray(100);
			long size = LargeArray.getMaxSizeOf32bitArray() + 2L;
			IOUtil.createLargeArray(size, false);
		} finally {
			LargeArray.setMaxSizeOf32bitArray(old);
		}
	}
}

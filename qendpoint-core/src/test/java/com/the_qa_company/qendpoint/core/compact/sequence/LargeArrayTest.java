package com.the_qa_company.qendpoint.core.compact.sequence;

import com.the_qa_company.qendpoint.core.unsafe.MemoryUtils;
import com.the_qa_company.qendpoint.core.unsafe.MemoryUtilsTest;
import org.junit.Test;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

public class LargeArrayTest {

	@Test
	public void allocationTest() {
		int old = MemoryUtils.getMaxArraySize();
		try {
			MemoryUtilsTest.setMaxArraySize(100);
			long size = MemoryUtils.getMaxArraySize() + 2L;
			IOUtil.createLargeArray(size, false);
		} finally {
			MemoryUtilsTest.setMaxArraySize(old);
		}
	}
}

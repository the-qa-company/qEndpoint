package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class StringUtilTest {

	@Test
	public void suffixTest() {
		String str = "/test:t:.txt";
		assertEquals(".txt", str.substring(StringUtil.lastIndexOf("/!:", str) + 1));
		assertEquals(".txt", IOUtil.getSuffix(str));

	}
}

package com.the_qa_company.qendpoint.core.search;

import com.the_qa_company.qendpoint.core.hdt.HDTFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SimpleQueryToolTest {
	HDTQueryTool tool;

	@Before
	public void setup() throws IOException {
		HDTQueryToolFactoryImpl factory = new HDTQueryToolFactoryImpl();
		tool = factory.newGenericQueryTool(HDTFactory.createHDT());
	}

	@Test
	public void prefixTest() {
		tool.registerPrefix("test", "http://example/test/");
		assertEquals("http://example/test/ok", tool.component("test:ok").stringValue());
	}
}

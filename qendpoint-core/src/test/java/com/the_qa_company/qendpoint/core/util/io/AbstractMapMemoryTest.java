package com.the_qa_company.qendpoint.core.util.io;

import org.junit.After;
import org.junit.Before;

/**
 * extend this class to all test using mapped memory to check if no unclose
 * mapped file are present.
 * <p>
 * This code is equivalent to add:
 *
 * <pre>
 * {@literal @Before}
 *  public void setup() {
 *      CloseMappedByteBufferUtil.markMapTest();
 *  }
 *
 * {@literal @After}
 *  public void complete() {
 *      CloseMappedByteBufferUtil.crashMapTest();
 *  }
 * </pre>
 *
 * @author Antoine Willerval
 */
public class AbstractMapMemoryTest {
	@Before
	public void setupMapMemoryTest() {
		CloseMappedByteBufferUtil.markMapTest();
	}

	@After
	public void completeMapMemoryTest() {
		CloseMappedByteBufferUtil.crashMapTest();
	}
}

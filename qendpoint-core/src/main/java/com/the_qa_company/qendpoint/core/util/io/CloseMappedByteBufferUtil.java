package com.the_qa_company.qendpoint.core.util.io;

/**
 * Utility class to trace unclosed mapped buffer.
 *
 * @author Antoine Willerval
 */
public class CloseMappedByteBufferUtil {
	/**
	 * start recording map tests
	 */
	public static void markMapTest() {
		CloseMappedByteBuffer.markMapTest();
	}

	/**
	 * end recording of map tests and crash if a mapped element wasn't closed
	 */
	public static void crashMapTest() {
		CloseMappedByteBuffer.crashMapTest();
	}
}

package com.the_qa_company.qendpoint.compiler;

/**
 * Utility class to change default values for
 * {@link com.the_qa_company.qendpoint.compiler.CompiledSailOptions}
 *
 * @author Antoine Willerval
 */
public class DebugOptionTestUtils {
	/**
	 * clear the debug option, should be done after each test changing the
	 * default options
	 */
	public static void clearDebugOption() {
		CompiledSailOptions.debugOptions = null;
	}

	/**
	 * get or create debug option, should call {@link #clearDebugOption()} after
	 * the end of the test
	 */
	public static CompiledSailOptions getOrCreateDebugOption() {
		if (CompiledSailOptions.debugOptions == null) {
			CompiledSailOptions.debugOptions = new CompiledSailOptions();
		}
		return CompiledSailOptions.debugOptions;
	}
}

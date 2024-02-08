package com.the_qa_company.qendpoint.compiler;

import java.io.IOException;

/**
 * Config option for the {@link SailCompiler}.
 *
 * @author Antoine Willerval
 */
public interface SailCompilerConfig {
	/**
	 * Config the sail compiler
	 *
	 * @param compiler compiler
	 * @throws IOException io exception
	 */
	void config(SailCompiler compiler) throws IOException;
}

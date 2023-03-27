package com.the_qa_company.qendpoint.compiler;

import java.nio.file.Path;

/**
 * validation class for SailCompiler result
 *
 * @author Antoine Willerval
 */
public interface SailCompilerValidator {
	/**
	 * Validate a path value
	 *
	 * @param path the path
	 * @throws SailCompiler.SailCompilerException exception if the path isn't
	 *                                            valid
	 */
	default void validatePath(Path path) throws SailCompiler.SailCompilerException {
		// impl should check the path if required
	}
}

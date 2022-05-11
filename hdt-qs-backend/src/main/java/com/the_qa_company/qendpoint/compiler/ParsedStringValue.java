package com.the_qa_company.qendpoint.compiler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * an annotation to describe a value for the
 * {@link SailCompilerSchema#PARSED_STRING_DATATYPE}, it should be put
 * on a method returning a string and without parameters, the methods with this annotation would be called once the
 * {@link SailCompiler#registerDirObject(Object)} method is called.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ParsedStringValue {
	/**
	 * @return the id of the value
	 */
	String value();
}

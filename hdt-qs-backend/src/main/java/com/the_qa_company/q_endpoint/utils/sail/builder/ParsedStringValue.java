package com.the_qa_company.q_endpoint.utils.sail.builder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ParsedStringValue {
	/**
	 * @return the id of the value
	 */
	String value();
}

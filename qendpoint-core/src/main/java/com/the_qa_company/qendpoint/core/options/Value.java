package com.the_qa_company.qendpoint.core.options;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Describe the value of a {@link Key} of type {@link Key.Type#ENUM}
 *
 * @author Antoine Willerval
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Value {
	/**
	 * @return the key
	 */
	String key();

	/**
	 * @return description of the value
	 */
	String desc() default "";
}

package com.the_qa_company.qendpoint.core.options;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * define a key in the HDTOptionsKey class
 *
 * @author Antoine Willerval
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Key {
	/**
	 * Type enum for a key
	 */
	enum Type {
		STRING("String"), PATH("Path"), NUMBER("Number"), DOUBLE("Double"), BOOLEAN("Boolean"), ENUM("Enum");

		private final String title;

		Type(String title) {
			this.title = title;
		}

		public String getTitle() {
			return title;
		}
	}

	/**
	 * @return description of the key
	 */
	String desc() default "";

	/**
	 * @return type of the key
	 */
	Type type() default Type.STRING;
}

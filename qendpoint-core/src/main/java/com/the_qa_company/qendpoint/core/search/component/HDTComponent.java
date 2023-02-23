package com.the_qa_company.qendpoint.core.search.component;

/**
 * Component in an HDT search query
 *
 * @author Antoine Willerval
 */
public interface HDTComponent {
	/**
	 * @return true if this component is a constant, false otherwise
	 */
	default boolean isConstant() {
		return false;
	}

	/**
	 * @return true if this component is a variable, false otherwise
	 */
	default boolean isVariable() {
		return false;
	}

	/**
	 * @return string value of this component
	 */
	String stringValue();

	/**
	 * @return this component as a constant
	 * @throws IllegalArgumentException if this component isn't a constant
	 * @see #isConstant()
	 */
	default HDTConstant asConstant() {
		throw new IllegalArgumentException("This pattern component isn't a constant!");
	}

	/**
	 * @return this component as a variable
	 * @throws IllegalArgumentException if this component isn't a variable
	 * @see #isVariable()
	 */
	default HDTVariable asVariable() {
		throw new IllegalArgumentException("This pattern component isn't a variable!");
	}

	/**
	 * @return a copy of this component
	 */
	HDTComponent copy();
}

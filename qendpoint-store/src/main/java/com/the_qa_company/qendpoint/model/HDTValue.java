package com.the_qa_company.qendpoint.model;

/**
 * Class to describe HDT value
 *
 * @author Antoine Willerval
 */
public interface HDTValue {
	/**
	 * @return is a delegate value, should return the exact hashcode when asked
	 *         if true
	 */
	boolean isDelegate();

	/**
	 * set if this value is a delegate value, it should return the exact
	 * hashcode when used
	 *
	 * @param delegate boolean
	 */
	void setDelegate(boolean delegate);
}

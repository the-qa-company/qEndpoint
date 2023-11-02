package com.the_qa_company.qendpoint.model;

/**
 * Class to describe HDT value
 *
 * @author Antoine Willerval
 */
public interface HDTValue {
	static int compare(HDTValue v1, HDTValue v2) {
		int c = Integer.compare(v1.getHDTPosition(), v2.getHDTPosition());

		if (c != 0) {
			return c;
		}

		return Long.compare(v1.getHDTPosition(), v2.getHDTPosition());
	}

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

	/**
	 * @return the id inside the hdt section, 0 or negative for invalid ids
	 */
	long getHDTId();

	/**
	 * @return the section id of the hdt value
	 */
	int getHDTPosition();

	/**
	 * @return if the HDT id is valid
	 */
	default boolean isValidHDTId() {
		return getHDTId() > 0;
	}
}

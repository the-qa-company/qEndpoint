package com.the_qa_company.qendpoint.core.util.io;

import java.io.IOException;

/**
 * Interface to add a function to check for the integrity of an object, it can
 * be used for the mapped structures.
 *
 * @author Antoine Willerval
 */
public interface IntegrityObject {
	/**
	 * check if an object is an {@link IntegrityObject} and call
	 * {@link #checkIntegrity()} on it.
	 *
	 * @param obj the object
	 * @throws IOException same as checkIntegrity
	 */
	static void checkObjectIntegrity(Object obj) throws IOException {
		if (obj instanceof IntegrityObject io) {
			io.checkIntegrity();
		}
	}

	/**
	 * call {@link #checkObjectIntegrity(Object)} on multiple objects.
	 *
	 * @param objs the objects
	 * @throws IOException same as checkObjectIntegrity
	 */
	static void checkAllIntegrity(Object... objs) throws IOException {
		for (Object o : objs) {
			checkObjectIntegrity(o);
		}
	}

	/**
	 * check for the integrity of this object.
	 *
	 * @throws IOException integrity or read exception
	 */
	void checkIntegrity() throws IOException;
}

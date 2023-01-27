package com.the_qa_company.qendpoint.core.util;

import java.util.function.Supplier;

/**
 * Simple object reference
 *
 * @param <T> type of the object
 */
public class Reference<T> {
	private T object;

	/**
	 * create with an object
	 *
	 * @param object the object
	 */
	public Reference(T object) {
		this.object = object;
	}

	/**
	 * create with a null object
	 */
	public Reference() {
		this(null);
	}

	/**
	 * set the object
	 *
	 * @param object the object
	 */
	public void setObject(T object) {
		this.object = object;
	}

	/**
	 * @return the object
	 */
	public T getObject() {
		return object;
	}

	/**
	 * @return if the object is null
	 */
	public boolean isNull() {
		return object == null;
	}

	/**
	 * compute the object if it is null and return the objec
	 *
	 * @param compute the compute function
	 * @return the object
	 */
	public T computeIfAbsent(Supplier<T> compute) {
		if (isNull()) {
			setObject(compute.get());
		}
		return getObject();
	}
}

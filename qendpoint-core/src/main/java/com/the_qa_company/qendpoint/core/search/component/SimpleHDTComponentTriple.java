package com.the_qa_company.qendpoint.core.search.component;

import java.util.Objects;

public class SimpleHDTComponentTriple implements HDTComponentTriple {
	private final HDTComponent subject;
	private final HDTComponent predicate;
	private final HDTComponent object;

	public SimpleHDTComponentTriple(HDTComponent subject, HDTComponent predicate, HDTComponent object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	public SimpleHDTComponentTriple(HDTComponentTriple triple) {
		this(triple.getSubject(), triple.getPredicate(), triple.getObject());
	}

	/**
	 * @return subject component
	 */
	@Override
	public HDTComponent getSubject() {
		return subject;
	}

	/**
	 * @return predicate component
	 */
	@Override
	public HDTComponent getPredicate() {
		return predicate;
	}

	/**
	 * @return object component
	 */
	@Override
	public HDTComponent getObject() {
		return object;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		HDTComponentTriple that = (HDTComponentTriple) o;
		return Objects.equals(subject, that.getSubject()) && Objects.equals(predicate, that.getPredicate())
				&& Objects.equals(object, that.getObject());
	}

	@Override
	public int hashCode() {
		return Objects.hash(subject, predicate, object);
	}

	@Override
	public String toString() {
		return subject + " " + predicate + " " + object + " .";
	}

}

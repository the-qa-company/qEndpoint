package com.the_qa_company.qendpoint.core.search.component;

public class HDTComponentTripleWrapper implements HDTComponentTriple {
	private final HDTComponentTriple handle;

	public HDTComponentTripleWrapper(HDTComponentTriple handle) {
		this.handle = handle;
	}

	public HDTComponentTriple getHandle() {
		return handle;
	}

	@Override
	public HDTComponent getSubject() {
		return handle.getSubject();
	}

	@Override
	public HDTComponent getPredicate() {
		return handle.getPredicate();
	}

	@Override
	public HDTComponent getObject() {
		return handle.getObject();
	}

	@Override
	public String toString() {
		return getSubject() + " " + getPredicate() + " " + getObject();
	}
}

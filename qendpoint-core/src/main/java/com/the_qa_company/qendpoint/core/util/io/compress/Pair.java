package com.the_qa_company.qendpoint.core.util.io.compress;

public class Pair implements Cloneable {
	public long predicatePosition;
	public long object;
	public long predicate;

	public void setAll(long predicatePosition, long object, long predicate) {
		this.predicatePosition = predicatePosition;
		this.object = object;
		this.predicate = predicate;
	}

	public void setAll(Pair pair) {
		predicatePosition = pair.predicatePosition;
		object = pair.object;
		predicate = pair.predicate;
	}

	public void increaseAll(long predicatePosition, long object, long predicate) {
		this.predicatePosition += predicatePosition;
		this.object += object;
		this.predicate += predicate;
	}

	@Override
	public Pair clone() {
		try {
			return (Pair) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new AssertionError();
		}
	}
}

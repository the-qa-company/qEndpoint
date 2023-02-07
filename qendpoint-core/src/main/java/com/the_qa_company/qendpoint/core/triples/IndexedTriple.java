package com.the_qa_company.qendpoint.core.triples;

/**
 * A triple of {@link IndexedNode}
 *
 * @author Antoine Willerval
 */
public class IndexedTriple {
	private IndexedNode subject;
	private IndexedNode predicate;
	private IndexedNode object;

	public IndexedTriple(IndexedNode subject, IndexedNode predicate, IndexedNode object) {
		load(subject, predicate, object);
	}

	public IndexedNode getSubject() {
		return subject;
	}

	public IndexedNode getPredicate() {
		return predicate;
	}

	public IndexedNode getObject() {
		return object;
	}

	/**
	 * load a new s p o inside this triple
	 *
	 * @param subject   the subject
	 * @param predicate the predicate
	 * @param object    the object
	 */
	public void load(IndexedNode subject, IndexedNode predicate, IndexedNode object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

}

package com.the_qa_company.qendpoint.core.storage.search;

import com.the_qa_company.qendpoint.core.storage.QEPComponent;

import java.util.Objects;

public class QEPComponentTripleSimple implements QEPComponentTriple {
	private long id;
	private QEPComponent subject;
	private QEPComponent predicate;
	private QEPComponent object;

	public QEPComponentTripleSimple(QEPComponent subject, QEPComponent predicate, QEPComponent object, long id) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.id = id;
	}

	public QEPComponentTripleSimple(QEPComponent subject, QEPComponent predicate, QEPComponent object) {
		this(subject, predicate, object, 0);
	}

	public QEPComponentTripleSimple() {
		this(null, null, null, 0);
	}

	public QEPComponent getSubject() {
		return subject;
	}

	public QEPComponent getPredicate() {
		return predicate;
	}

	public QEPComponent getObject() {
		return object;
	}

	public long getId() {
		return id;
	}

	public void setSubject(QEPComponent subject) {
		this.subject = subject;
	}

	public void setPredicate(QEPComponent predicate) {
		this.predicate = predicate;
	}

	public void setObject(QEPComponent object) {
		this.object = object;
	}
	public void setId(long id) {
		this.id = id;
	}

	public void setAll(QEPComponent subject, QEPComponent predicate, QEPComponent object) {
		setSubject(subject);
		setPredicate(predicate);
		setObject(object);
	}

	public void setAll(QEPComponent subject, QEPComponent predicate, QEPComponent object, long id) {
		setSubject(subject);
		setPredicate(predicate);
		setObject(object);
		setId(id);
	}

	@Override
	public String toString() {
		return getSubject() + " " + getPredicate() + " " + getObject();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof QEPComponentTriple that)) return false;
		return Objects.equals(getSubject(), that.getSubject()) && Objects.equals(getSubject(), that.getPredicate()) && Objects.equals(getObject(), that.getObject());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getSubject(), getPredicate(), getObject());
	}
}

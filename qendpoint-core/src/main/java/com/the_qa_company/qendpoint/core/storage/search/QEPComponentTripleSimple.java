package com.the_qa_company.qendpoint.core.storage.search;

import com.the_qa_company.qendpoint.core.storage.QEPComponent;

import java.util.Objects;

public class QEPComponentTripleSimple implements QEPComponentTriple {
	private long id;
	private int datasetId;
	private QEPComponent subject;
	private QEPComponent predicate;
	private QEPComponent object;

	public QEPComponentTripleSimple(QEPComponent subject, QEPComponent predicate, QEPComponent object, long id,
			int datasetId) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.id = id;
		this.datasetId = datasetId;
	}

	public QEPComponentTripleSimple(QEPComponent subject, QEPComponent predicate, QEPComponent object, long id) {
		this(subject, predicate, object, id, 0);
	}

	public QEPComponentTripleSimple(QEPComponent subject, QEPComponent predicate, QEPComponent object) {
		this(subject, predicate, object, 0);
	}

	public QEPComponentTripleSimple() {
		this(null, null, null, 0);
	}

	@Override
	public QEPComponent getSubject() {
		return subject;
	}

	@Override
	public QEPComponent getPredicate() {
		return predicate;
	}

	@Override
	public QEPComponent getObject() {
		return object;
	}

	@Override
	public long getId() {
		return id;
	}

	@Override
	public int getDatasetId() {
		return datasetId;
	}

	@Override
	public void setSubject(QEPComponent subject) {
		this.subject = subject;
	}

	@Override
	public void setPredicate(QEPComponent predicate) {
		this.predicate = predicate;
	}

	@Override
	public void setObject(QEPComponent object) {
		this.object = object;
	}

	@Override
	public void setId(long id) {
		this.id = id;
	}

	@Override
	public void setDatasetId(int datasetId) {
		this.datasetId = datasetId;
	}

	@Override
	public String toString() {
		return getSubject() + " " + getPredicate() + " " + getObject();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof QEPComponentTriple that))
			return false;
		return Objects.equals(getSubject(), that.getSubject()) && Objects.equals(getSubject(), that.getPredicate())
				&& Objects.equals(getObject(), that.getObject());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getSubject(), getPredicate(), getObject());
	}
}

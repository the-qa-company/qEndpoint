package com.the_qa_company.qendpoint.core.storage.search;

import com.the_qa_company.qendpoint.core.storage.QEPComponent;

import java.util.Objects;

/**
 * a triple with frozen components, all non-null components won't be mutable
 * anymore
 */
public class QEPComponentTripleFreeze implements QEPComponentTriple {
	private final QEPComponentTriple wrapper;
	private final QEPComponent subject;
	private final QEPComponent predicate;
	private final QEPComponent object;

	private QEPComponentTripleFreeze(QEPComponentTriple wrapper, QEPComponent subject, QEPComponent predicate,
			QEPComponent object) {
		this.wrapper = wrapper;
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	public QEPComponentTripleFreeze(QEPComponent subject, QEPComponent predicate, QEPComponent object) {
		this(QEPComponentTriple.of(), subject, predicate, object);
	}

	@Override
	public QEPComponent getSubject() {
		return subject == null ? wrapper.getSubject() : subject;
	}

	@Override
	public QEPComponent getPredicate() {
		return predicate == null ? wrapper.getPredicate() : predicate;
	}

	@Override
	public QEPComponent getObject() {
		return object == null ? wrapper.getObject() : object;
	}

	@Override
	public long getId() {
		return wrapper.getId();
	}

	@Override
	public int getDatasetId() {
		return wrapper.getDatasetId();
	}

	@Override
	public void setSubject(QEPComponent subject) {
		if (this.subject == null) {
			wrapper.setSubject(subject);
		}
	}

	@Override
	public void setPredicate(QEPComponent predicate) {
		if (this.predicate == null) {
			wrapper.setPredicate(predicate);
		}
	}

	@Override
	public void setObject(QEPComponent object) {
		if (this.object == null) {
			wrapper.setObject(object);
		}
	}

	@Override
	public void setId(long id) {
		wrapper.setId(id);
	}

	@Override
	public void setDatasetId(int id) {
		wrapper.setDatasetId(id);
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

	@Override
	public QEPComponentTriple deepClone() {
		QEPComponent s = subject != null ? subject.clone() : null;
		QEPComponent p = predicate != null ? predicate.clone() : null;
		QEPComponent o = object != null ? object.clone() : null;
		return new QEPComponentTripleFreeze(wrapper.deepClone(), s, p, o);
	}
}

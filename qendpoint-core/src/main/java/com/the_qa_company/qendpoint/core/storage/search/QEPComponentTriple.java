package com.the_qa_company.qendpoint.core.storage.search;

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.storage.QEPComponent;
import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import com.the_qa_company.qendpoint.core.storage.QEPDataset;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;

/**
 * A triple of {@link QEPComponent}
 *
 * @author Antoine Willerval
 */
public interface QEPComponentTriple {

	/**
	 * create a mutable triple
	 *
	 * @return triple
	 */
	static QEPComponentTriple of() {
		return new QEPComponentTripleSimple();
	}

	/**
	 * create a mutable triple
	 *
	 * @param s subject
	 * @param p predicate
	 * @param o object
	 * @return triple
	 */
	static QEPComponentTriple of(QEPComponent s, QEPComponent p, QEPComponent o) {
		return new QEPComponentTripleSimple(s, p, o);
	}

	/**
	 * create a mutable triple
	 *
	 * @param s  subject
	 * @param p  predicate
	 * @param o  object
	 * @param id triple id
	 * @return triple
	 */
	static QEPComponentTriple of(QEPComponent s, QEPComponent p, QEPComponent o, long id) {
		return new QEPComponentTripleSimple(s, p, o, id);
	}

	/**
	 * @return the subject
	 */
	QEPComponent getSubject();

	/**
	 * @return the predicate
	 */
	QEPComponent getPredicate();

	/**
	 * @return the object
	 */
	QEPComponent getObject();

	/**
	 * @return get the triple id (if available)
	 */
	long getId();

	/**
	 * @return the dataset id
	 */
	int getDatasetId();

	/**
	 * set the subject
	 *
	 * @param subject subject
	 */
	void setSubject(QEPComponent subject);

	/**
	 * set the predicate
	 *
	 * @param predicate predicate
	 */
	void setPredicate(QEPComponent predicate);

	/**
	 * set the object
	 *
	 * @param object object
	 */
	void setObject(QEPComponent object);

	/**
	 * set the id
	 *
	 * @param id id
	 */
	void setId(long id);

	/**
	 * set the dataset id
	 *
	 * @param id id
	 */
	void setDatasetId(int id);

	/**
	 * set all the components
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 */
	default void setAll(QEPComponent subject, QEPComponent predicate, QEPComponent object) {
		setSubject(subject);
		setPredicate(predicate);
		setObject(object);
	}

	/**
	 * set all the components
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @param id        triple id
	 */
	default void setAll(QEPComponent subject, QEPComponent predicate, QEPComponent object, long id, int datasetId) {
		setSubject(subject);
		setPredicate(predicate);
		setObject(object);
		setId(id);
		setDatasetId(datasetId);
	}

	/**
	 * @return convert this triple to an overlay, all the non-null elements
	 *         won't be updatable anymore
	 */
	default QEPComponentTriple freeze() {
		return new QEPComponentTripleFreeze(getSubject(), getPredicate(), getObject());
	}

	/**
	 * convert this triple into a triple id for a particular dataset
	 *
	 * @param dataset dataset
	 * @return triple id
	 * @throws QEPCoreException exception while converting the ID
	 */
	default TripleID tripleID(QEPDataset dataset) throws QEPCoreException {
		QEPComponent s = getSubject();
		QEPComponent p = getPredicate();
		QEPComponent o = getObject();

		long sid, pid, oid;

		if (s == null) {
			sid = 0;
		} else {
			long id = s.getId(dataset.uid(), TripleComponentRole.SUBJECT);
			if (id == 0) {
				sid = -1;
			} else {
				sid = id;
			}
		}

		if (p == null) {
			pid = 0;
		} else {
			long id = p.getId(dataset.uid(), TripleComponentRole.PREDICATE);
			if (id == 0) {
				pid = -1;
			} else {
				pid = id;
			}
		}

		if (o == null) {
			oid = 0;
		} else {
			long id = o.getId(dataset.uid(), TripleComponentRole.OBJECT);
			if (id == 0) {
				oid = -1;
			} else {
				oid = id;
			}
		}

		return new TripleID(sid, pid, oid);
	}

	/**
	 * @return convert to triple string
	 */
	default TripleString tripleString() {
		QEPComponent subject = getSubject();
		QEPComponent predicate = getPredicate();
		QEPComponent object = getObject();
		return new TripleString(subject == null ? "" : subject.getString(),
				predicate == null ? "" : predicate.getString(), object == null ? "" : object.getString());
	}

	@Override
	String toString();

	@Override
	boolean equals(Object o);

	@Override
	int hashCode();

	/**
	 * @return a deep clone of this triple, the component will be cloned
	 */
	QEPComponentTriple deepClone();
}

package com.the_qa_company.qendpoint.core.storage.search;

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.storage.QEPComponent;
import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import com.the_qa_company.qendpoint.core.storage.QEPDataset;
import com.the_qa_company.qendpoint.core.triples.TripleID;

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
	default void setAll(QEPComponent subject, QEPComponent predicate, QEPComponent object, long id) {
		setSubject(subject);
		setPredicate(predicate);
		setObject(object);
		setId(id);
	}

	/**
	 * @return convert this triple to an overlay, all the non-null elements won't be updatable anymore
	 */
	default QEPComponentTriple freeze() {
		return new QEPComponentTripleFreeze(getSubject(), getPredicate(), getObject());
	}

	default TripleID tripleID(QEPDataset dataset) throws QEPCoreException {
		QEPComponent s = getSubject();
		QEPComponent p = getPredicate();
		QEPComponent o = getObject();
		return new TripleID(
				s == null ? 0 : s.getId(dataset.uid(), TripleComponentRole.SUBJECT),
				p == null ? 0 : p.getId(dataset.uid(), TripleComponentRole.PREDICATE),
				o == null ? 0 : o.getId(dataset.uid(), TripleComponentRole.OBJECT)
		);
	}

	@Override
	String toString();

	@Override
	boolean equals(Object o);

	@Override
	int hashCode();
}

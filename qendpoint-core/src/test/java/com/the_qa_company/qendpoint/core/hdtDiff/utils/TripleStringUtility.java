package com.the_qa_company.qendpoint.core.hdtDiff.utils;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;

/**
 * Comparable TripleChar, load {@link TripleID} into string for sorted load
 *
 * @author ate47
 */
public class TripleStringUtility {
	private final Dictionary dict;
	private long oldSubject = -2L;
	private long oldPredicate = -2L;
	private long oldObject = -2L;
	private CharSequence subject;
	private CharSequence predicate;
	private CharSequence object;

	public TripleStringUtility(HDT hdt) {
		this.dict = hdt.getDictionary();
	}

	/**
	 * load a triple inside this triple
	 *
	 * @param subj the subject
	 * @param pred the predicate
	 * @param obje the object
	 */
	public void loadTriple(long subj, long pred, long obje) {

		if (subj != oldSubject) {
			oldSubject = subj;
			subject = dict.idToString(subj, TripleComponentRole.SUBJECT);
		}
		if (pred != oldPredicate) {
			oldPredicate = pred;
			predicate = dict.idToString(pred, TripleComponentRole.PREDICATE);
		}
		if (obje != oldObject) {
			oldObject = obje;
			object = dict.idToString(obje, TripleComponentRole.OBJECT);
		}
	}

	public void loadTriple(TripleID triple) {
		loadTriple(triple.getSubject(), triple.getPredicate(), triple.getObject());
	}

	/**
	 * search this triple into another hdt
	 *
	 * @param other the other hdt
	 * @return triple in the other hdt or null
	 */
	public TripleID searchInto(HDT other) {
		Dictionary dict2 = other.getDictionary();
		TripleID id = new TripleID(dict2.stringToId(subject, TripleComponentRole.SUBJECT),
				dict2.stringToId(predicate, TripleComponentRole.PREDICATE),
				dict2.stringToId(object, TripleComponentRole.OBJECT));
		IteratorTripleID it = other.getTriples().search(id);
		return it.hasNext() ? it.next() : null;
	}

	@Override
	public String toString() {
		return "(" + subject + "," + predicate + "," + object + ")";
	}

	public CharSequence getObject() {
		return object;
	}
}

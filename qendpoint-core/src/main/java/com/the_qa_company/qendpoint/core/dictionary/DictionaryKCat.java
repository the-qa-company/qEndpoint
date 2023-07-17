package com.the_qa_company.qendpoint.core.dictionary;

import java.util.Map;

public interface DictionaryKCat {

	/**
	 * @return the subsections to merge
	 */
	Map<CharSequence, DictionarySection> getSubSections();

	/**
	 * @return the subject section
	 */
	DictionarySection getSubjectSection();

	/**
	 * @return the object section
	 */
	DictionarySection getObjectSection();

	/**
	 * @return the predicate section
	 */
	DictionarySection getPredicateSection();

	/**
	 * @return the shared section
	 */
	DictionarySection getSharedSection();

	/**
	 * @return the number of subjects
	 */
	long countSubjects();

	/**
	 * @return the number of shared
	 */
	long countShared();

	/**
	 * @return the number of predicates
	 */
	long countPredicates();

	/**
	 * @return the number of objects
	 */
	long countObjects();

	/**
	 * @return the object shift in the dictionary IDs
	 */
	long nonTypedShift();

	/**
	 * @return the object shift in the dictionary IDs
	 */
	long typedShift();
}

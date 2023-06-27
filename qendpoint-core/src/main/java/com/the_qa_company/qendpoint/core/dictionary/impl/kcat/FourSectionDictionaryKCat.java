package com.the_qa_company.qendpoint.core.dictionary.impl.kcat;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryKCat;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;

import java.util.Collections;
import java.util.Map;

public class FourSectionDictionaryKCat implements DictionaryKCat {
	private final Dictionary dictionary;

	public FourSectionDictionaryKCat(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

	@Override
	public Map<CharSequence, DictionarySection> getSubSections() {
		return Collections.emptyMap();
	}

	@Override
	public DictionarySection getSubjectSection() {
		return dictionary.getSubjects();
	}

	@Override
	public DictionarySection getObjectSection() {
		return dictionary.getObjects();
	}

	@Override
	public DictionarySection getPredicateSection() {
		return dictionary.getPredicates();
	}

	@Override
	public DictionarySection getSharedSection() {
		return dictionary.getShared();
	}

	@Override
	public long countSubjects() {
		return dictionary.getSubjects().getNumberOfElements() + countShared();
	}

	@Override
	public long countShared() {
		return dictionary.getShared().getNumberOfElements();
	}

	@Override
	public long countPredicates() {
		return dictionary.getPredicates().getNumberOfElements();
	}

	@Override
	public long countObjects() {
		return dictionary.getObjects().getNumberOfElements() + countShared();
	}

	@Override
	public long nonTypedShift() {
		return countShared();
	}

	@Override
	public long typedShift() {
		return 0;
	}
}

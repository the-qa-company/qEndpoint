package com.the_qa_company.qendpoint.core.dictionary.impl.kcat;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryKCat;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;

import java.util.Map;
import java.util.TreeMap;

public class MultipleSectionLangPrefixesDictionaryKCat implements DictionaryKCat {
	private final Dictionary dictionary;
	private final Map<? extends CharSequence, DictionarySection> allObjects;

	public MultipleSectionLangPrefixesDictionaryKCat(Dictionary dictionary) {
		this.dictionary = dictionary;
		allObjects = dictionary.getAllObjects();
	}

	@Override
	public Map<CharSequence, DictionarySection> getSubSections() {
		Map<CharSequence, DictionarySection> sections = new TreeMap<>(CharSequenceComparator.getInstance());
		allObjects.forEach((key, section) -> {
			if (!LiteralsUtils.NO_DATATYPE.equals(key)) {
				// we ignore this section because it will be used in the shared
				// compute
				sections.put(ByteString.of(key), section);
			}
		});
		return sections;
	}

	@Override
	public DictionarySection getSubjectSection() {
		return dictionary.getSubjects();
	}

	@Override
	public DictionarySection getPredicateSection() {
		return dictionary.getPredicates();
	}

	@Override
	public DictionarySection getGraphSection() {
		return dictionary.getGraphs();
	}

	@Override
	public DictionarySection getObjectSection() {
		return allObjects.get(LiteralsUtils.NO_DATATYPE);
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
		long count = 0;
		for (DictionarySection sec : allObjects.values()) {
			count += sec.getNumberOfElements();
		}
		return count + countShared();
	}

	@Override
	public long nonTypedShift() {
		// NDT are first
		return countShared();
	}

	@Override
	public long countGraphs() {
		return dictionary.supportGraphs() ? dictionary.getGraphs().getNumberOfElements() : 0;
	}

	@Override
	public long typedShift() {
		DictionarySection section = getObjectSection();
		if (section == null) {
			return nonTypedShift();
		}
		return nonTypedShift() + section.getNumberOfElements();
	}
}

package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySectionMap;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCOptimizedExtractor;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.SectionUtil;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class MultDictionaryPFCOptimizedExtractor implements OptimizedExtractor {
	private final PFCOptimizedExtractor shared, subjects, predicates;
	private final TreeMap<ByteString, PFCOptimizedExtractor> objects;
	private final long numshared;

	public MultDictionaryPFCOptimizedExtractor(MultipleBaseDictionary origDict) {
		numshared = (int) origDict.getNshared();
		shared = new PFCOptimizedExtractor((PFCDictionarySectionMap) origDict.shared);
		subjects = new PFCOptimizedExtractor((PFCDictionarySectionMap) origDict.subjects);
		predicates = new PFCOptimizedExtractor((PFCDictionarySectionMap) origDict.predicates);
		objects = new TreeMap<>(CharSequenceComparator.getInstance());
		for (Map.Entry<? extends CharSequence, DictionarySection> entry : origDict.getAllObjects().entrySet()) {
			objects.put(ByteString.of(entry.getKey()),
					new PFCOptimizedExtractor((PFCDictionarySectionMap) entry.getValue()));
		}
	}

	@Override
	public CharSequence idToString(long id, TripleComponentRole role) {
		AbstractMap.SimpleEntry<ByteString, PFCOptimizedExtractor> section = getSection(id, role);
		long localId = getLocalId(id, role);
		if (section.getKey().equals(LiteralsUtils.NO_DATATYPE) || section.getKey().equals(SectionUtil.SECTION))
			return section.getValue().extract(localId);
		else {
			String label = section.getValue().extract(localId).toString();
			ByteString dType = section.getKey();
			// Matcher matcher = pattern.matcher(label);
			if (LiteralsUtils.containsLanguage(label)) {
				return label;
			} else {
				return label + "^^" + dType;
			}
		}
	}

	private AbstractMap.SimpleEntry<ByteString, PFCOptimizedExtractor> getSection(long id, TripleComponentRole role) {
		switch (role) {
		case SUBJECT:
			if (id <= numshared) {
				return new AbstractMap.SimpleEntry<>(SectionUtil.SECTION, shared);
			} else {
				return new AbstractMap.SimpleEntry<>(SectionUtil.SECTION, subjects);
			}
		case PREDICATE:
			return new AbstractMap.SimpleEntry<>(SectionUtil.SECTION, predicates);
		case OBJECT:
			if (id <= numshared) {
				return new AbstractMap.SimpleEntry<>(SectionUtil.SECTION, shared);
			} else {
				Iterator<Map.Entry<ByteString, PFCOptimizedExtractor>> hmIterator = objects.entrySet().iterator();
				// iterate over all subsections in the objects section
				PFCOptimizedExtractor desiredSection = null;
				ByteString type = ByteString.empty();
				int count = 0;
				while (hmIterator.hasNext()) {
					Map.Entry<ByteString, PFCOptimizedExtractor> entry = hmIterator.next();
					PFCOptimizedExtractor subSection = entry.getValue();
					count += subSection.getNumStrings();
					if (id <= numshared + count) {
						desiredSection = subSection;
						type = entry.getKey();
						break;
					}
				}
				return new AbstractMap.SimpleEntry<>(type, desiredSection);
			}
		}
		throw new IllegalArgumentException();
	}

	private long getLocalId(long id, TripleComponentRole position) {
		switch (position) {
		case SUBJECT:
			if (id <= numshared)
				return id;
			else
				return id - numshared;
		case OBJECT:
			if (id <= numshared) {
				return id;
			} else {
				Iterator<Map.Entry<ByteString, PFCOptimizedExtractor>> hmIterator = objects.entrySet().iterator();
				// iterate over all subsections in the objects section
				long count = 0;
				while (hmIterator.hasNext()) {
					Map.Entry<ByteString, PFCOptimizedExtractor> entry = hmIterator.next();
					PFCOptimizedExtractor subSection = entry.getValue();
					count += subSection.getNumStrings();
					if (id <= numshared + count) {
						count -= subSection.getNumStrings();
						break;
					}
				}
				// subtract the number of elements in the shared + the
				// subsections in the objects section
				return id - count - numshared;
			}
		case PREDICATE:
			return id;
		}

		throw new IllegalArgumentException();
	}
}

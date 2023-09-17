package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySectionMap;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCOptimizedExtractor;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;

public class DictionaryPFCOptimizedExtractor implements OptimizedExtractor {
	private final PFCOptimizedExtractor shared, subjects, predicates, objects, graphs;
	private final long numshared;

	public DictionaryPFCOptimizedExtractor(BaseDictionary origDict) {
		numshared = origDict.getNshared();
		shared = new PFCOptimizedExtractor((PFCDictionarySectionMap) origDict.shared);
		subjects = new PFCOptimizedExtractor((PFCDictionarySectionMap) origDict.subjects);
		predicates = new PFCOptimizedExtractor((PFCDictionarySectionMap) origDict.predicates);
		objects = new PFCOptimizedExtractor((PFCDictionarySectionMap) origDict.objects);
		if (origDict.graphs == null) {
			graphs = null;
		} else {
			graphs = new PFCOptimizedExtractor((PFCDictionarySectionMap) origDict.graphs);
		}
	}

	public CharSequence idToString(long id, TripleComponentRole role) {
		PFCOptimizedExtractor section = getSection(id, role);
		long localId = getLocalId(id, role);
		return section.extract(localId);
	}

	private PFCOptimizedExtractor getSection(long id, TripleComponentRole role) {
		switch (role) {
		case SUBJECT:
			if (id <= numshared) {
				return shared;
			} else {
				return subjects;
			}
		case PREDICATE:
			return predicates;
		case OBJECT:
			if (id <= numshared) {
				return shared;
			} else {
				return objects;
			}
		case GRAPH:
			return graphs;
		}
		throw new IllegalArgumentException();
	}

	private long getLocalId(long id, TripleComponentRole position) {
		switch (position) {
		case SUBJECT:
		case OBJECT:
			if (id <= numshared) {
				return id;
			} else {
				return id - numshared;
			}
		case PREDICATE:
			return id;
		case GRAPH:
			return id;
		}

		throw new IllegalArgumentException();
	}
}

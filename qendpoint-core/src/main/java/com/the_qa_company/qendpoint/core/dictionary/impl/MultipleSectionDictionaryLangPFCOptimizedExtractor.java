package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySectionMap;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCOptimizedExtractor;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;

public class MultipleSectionDictionaryLangPFCOptimizedExtractor implements OptimizedExtractor {
	private final PFCOptimizedExtractor shared, subjects, predicates, graph;
	private final PFCOptimizedExtractor[] objects;
	private final MultipleLangBaseDictionary dict;
	private final long nshared;

	public MultipleSectionDictionaryLangPFCOptimizedExtractor(MultipleLangBaseDictionary dict) {
		this.dict = dict;
		nshared = dict.getNshared();
		shared = new PFCOptimizedExtractor((PFCDictionarySectionMap) dict.shared);
		subjects = new PFCOptimizedExtractor((PFCDictionarySectionMap) dict.subjects);
		predicates = new PFCOptimizedExtractor((PFCDictionarySectionMap) dict.predicates);
		objects = new PFCOptimizedExtractor[dict.getObjectsSectionCount()];
		for (int i = 0; i < objects.length; i++) {
			objects[i] = new PFCOptimizedExtractor((PFCDictionarySectionMap) dict.getObjectsSectionFromId(i).section());
		}
		if (dict.supportGraphs()) {
			graph = new PFCOptimizedExtractor((PFCDictionarySectionMap) dict.graph);
		} else {
			graph = null;
		}
	}

	@Override
	public CharSequence idToString(long id, TripleComponentRole role) {
		switch (role) {
		case SUBJECT -> {
			if (id <= nshared) {
				return shared.extract(id);
			} else {
				return subjects.extract(id - nshared);
			}
		}
		case PREDICATE -> {
			return predicates.extract(id);
		}
		case OBJECT -> {
			MultipleLangBaseDictionary.ObjectIdLocationData data = dict.idToObjectSection(id);
			if (data == null) {
				return null; // invalid id
			}
			CharSequence str = objects[data.uid()].extract(id - data.location());
			if (str == null) {
				return null;
			}
			return data.suffix().copyPreAppend(str);
		}
		case GRAPH -> {
			if (!dict.supportGraphs()) {
				throw new IllegalArgumentException("This dictionary doesn't support graphs!");
			}
			return graph.extract(id);
		}
		default -> throw new IllegalArgumentException("Bad role: " + role);
		}
	}
}

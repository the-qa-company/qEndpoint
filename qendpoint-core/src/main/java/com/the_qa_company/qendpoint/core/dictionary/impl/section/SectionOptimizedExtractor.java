package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;

public class SectionOptimizedExtractor implements SecOptimizedExtractor {
	private final DictionarySection sec;

	public SectionOptimizedExtractor(DictionarySection sec) {
		this.sec = sec;
	}


	@Override
	public CharSequence extract(long target) {
		return sec.extract(target);
	}

	@Override
	public long getNumStrings() {
		return sec.getNumberOfElements();
	}
}

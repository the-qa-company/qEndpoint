package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.TypedLiteralCompactString;

public class QuoteOptimizedExtractor implements SecOptimizedExtractor {
	private final DictionarySection section;

	public QuoteOptimizedExtractor(DictionarySection section) {
		this.section = section;
	}

	@Override
	public CharSequence extract(long target) {
		return new TypedLiteralCompactString(ByteString.of(section.extract(target)), null);
	}

	@Override
	public long getNumStrings() {
		return section.getNumberOfElements();
	}
}

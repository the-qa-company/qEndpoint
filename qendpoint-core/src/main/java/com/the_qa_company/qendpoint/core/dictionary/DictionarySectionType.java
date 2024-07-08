package com.the_qa_company.qendpoint.core.dictionary;

public enum DictionarySectionType {
	PFC(true), INT(false), FLOAT(false), DEC(false);

	private final boolean quotes;

	DictionarySectionType(boolean quotes) {
		this.quotes = quotes;
	}

	public boolean hasQuotes() {
		return quotes;
	}
}

package com.the_qa_company.qendpoint.core.enums;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;

import java.util.function.Function;
import java.util.function.Supplier;

public enum DictionarySectionRole {
	SUBJECT(() -> TripleComponentRole.SUBJECT, Dictionary::getSubjects),
	PREDICATE(() -> TripleComponentRole.PREDICATE, Dictionary::getPredicates),
	OBJECT(() -> TripleComponentRole.OBJECT, Dictionary::getObjects),
	SHARED(() -> TripleComponentRole.SUBJECT, Dictionary::getShared);

	private final Supplier<TripleComponentRole> roleSupplier;
	private final Function<Dictionary, DictionarySection> dictionarySectionFunction;
	private TripleComponentRole role;

	DictionarySectionRole(Supplier<TripleComponentRole> roleSupplier, Function<Dictionary, DictionarySection> dictionarySectionFunction) {
		this.roleSupplier = roleSupplier;
		this.dictionarySectionFunction = dictionarySectionFunction;
	}

	/**
	 * @return triple component role, if the dictionary role is shared, then the
	 *         role is subject
	 */
	public TripleComponentRole asTripleComponentRole() {
		if (role == null) {
			role = roleSupplier.get();
		}
		return role;
	}

	/**
	 * get this section from a dictionary
	 * @param dict dictionary
	 * @return section
	 */
	public DictionarySection getSection(Dictionary dict) {
		return dictionarySectionFunction.apply(dict);
	}
}

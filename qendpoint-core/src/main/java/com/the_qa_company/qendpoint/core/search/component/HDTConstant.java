package com.the_qa_company.qendpoint.core.search.component;

import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;

/**
 * Constant in an HDT search query
 *
 * @author Antoine Willerval
 */
public interface HDTConstant extends HDTComponent {
	@Override
	default boolean isConstant() {
		return true;
	}

	@Override
	default HDTConstant asConstant() {
		return this;
	}

	/**
	 * @return value of this constant as a CharSequence (or null if unavailable)
	 */
	CharSequence getValue();

	/**
	 * @return value of this constant as an ID (or -1 if unavailable)
	 */
	long getId(DictionarySectionRole role);

	default long getAsSubjectId() {
		return getId(DictionarySectionRole.SUBJECT);
	}

	default long getAsObjectId() {
		return getId(DictionarySectionRole.OBJECT);
	}

	default long getAsPredicate() {
		return getId(DictionarySectionRole.PREDICATE);
	}

	/**
	 * @return hash code of the stringValue
	 */
	@Override
	int hashCode();

	@Override
	boolean equals(Object other);

	/**
	 * @return a copy of this component
	 */
	@Override
	HDTConstant copy();
}

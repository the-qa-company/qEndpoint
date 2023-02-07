package com.the_qa_company.qendpoint.core.enums;

import java.util.function.Supplier;

public enum DictionarySectionRole {
	SUBJECT(() -> TripleComponentRole.SUBJECT), PREDICATE(() -> TripleComponentRole.PREDICATE),
	OBJECT(() -> TripleComponentRole.OBJECT), SHARED(() -> TripleComponentRole.SUBJECT);

	private final Supplier<TripleComponentRole> roleSupplier;
	private TripleComponentRole role;

	DictionarySectionRole(Supplier<TripleComponentRole> roleSupplier) {
		this.roleSupplier = roleSupplier;
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
}

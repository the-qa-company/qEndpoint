/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/enums
 * /TripleComponentRole.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License,
 * or (at your option) any later version. This library is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.enums;

import java.util.function.Supplier;

/**
 * Indicates the position of the triple, mainly in the dictionary
 */
public enum TripleComponentRole {
	/** The triple is a subject */
	SUBJECT(() -> DictionarySectionRole.SUBJECT),
	/** The triple is a predicate */
	PREDICATE(() -> DictionarySectionRole.PREDICATE),
	/** The triple is an object */
	OBJECT(() -> DictionarySectionRole.OBJECT);

	private DictionarySectionRole dictionarySectionRole;
	private final Supplier<DictionarySectionRole> dictionarySectionRoleSupplier;

	TripleComponentRole(Supplier<DictionarySectionRole> dictionarySectionRoleSupplier) {
		this.dictionarySectionRoleSupplier = dictionarySectionRoleSupplier;
	}

	public DictionarySectionRole asDictionarySectionRole() {
		// use supplier for cyclic dependency
		if (dictionarySectionRole == null) {
			dictionarySectionRole = dictionarySectionRoleSupplier.get();
		}
		return dictionarySectionRole;
	}
}

/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/enums
 * /TripleComponentOrder.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

/**
 * Indicates the order of the triples
 */
public enum TripleComponentOrder {
	/**
	 * Subject, predicate, object
	 */
	Unknown(null, null, null),
	/**
	 * Subject, predicate, object
	 */
	SPO(TripleComponentRole.SUBJECT, TripleComponentRole.PREDICATE, TripleComponentRole.OBJECT),
	/**
	 * Subject, object, predicate
	 */
	SOP(TripleComponentRole.SUBJECT, TripleComponentRole.OBJECT, TripleComponentRole.PREDICATE),
	/**
	 * Predicate, subject, object
	 */
	PSO(TripleComponentRole.PREDICATE, TripleComponentRole.SUBJECT, TripleComponentRole.OBJECT),
	/**
	 * Predicate, object, subject
	 */
	POS(TripleComponentRole.PREDICATE, TripleComponentRole.OBJECT, TripleComponentRole.SUBJECT),
	/**
	 * Object, subject, predicate
	 */
	OSP(TripleComponentRole.OBJECT, TripleComponentRole.SUBJECT, TripleComponentRole.PREDICATE),
	/**
	 * Object, predicate, subject
	 */
	OPS(TripleComponentRole.OBJECT, TripleComponentRole.PREDICATE, TripleComponentRole.SUBJECT);

	private final TripleComponentRole subjectMapping;
	private final TripleComponentRole predicateMapping;
	private final TripleComponentRole objectMapping;

	TripleComponentOrder(TripleComponentRole subjectMapping, TripleComponentRole predicateMapping,
			TripleComponentRole objectMapping) {
		this.subjectMapping = subjectMapping;
		this.predicateMapping = predicateMapping;
		this.objectMapping = objectMapping;
	}

	public TripleComponentRole getSubjectMapping() {
		return subjectMapping;
	}

	public TripleComponentRole getPredicateMapping() {
		return predicateMapping;
	}

	public TripleComponentRole getObjectMapping() {
		return objectMapping;
	}
}

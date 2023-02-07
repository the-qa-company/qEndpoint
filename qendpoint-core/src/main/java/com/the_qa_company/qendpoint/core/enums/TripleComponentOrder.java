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
	Unknown,
	/**
	 * Subject, predicate, object
	 */
	SPO,
	/**
	 * Subject, object, predicate
	 */
	SOP,
	/**
	 * Predicate, subject, object
	 */
	PSO,
	/**
	 * Predicate, object, subject
	 */
	POS,
	/**
	 * Object, subject, predicate
	 */
	OSP,
	/**
	 * Object, predicate, subject
	 */
	OPS,
}

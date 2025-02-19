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

import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

/**
 * Indicates the order of the triples
 */
public enum TripleComponentOrder {
	/**
	 * Subject, predicate, object
	 */
	Unknown(null, null, null, 0),
	/**
	 * Subject, predicate, object
	 */
	SPO(TripleComponentRole.SUBJECT, TripleComponentRole.PREDICATE, TripleComponentRole.OBJECT, 1),
	/**
	 * Subject, object, predicate
	 */
	SOP(TripleComponentRole.SUBJECT, TripleComponentRole.OBJECT, TripleComponentRole.PREDICATE, 1 << 1),
	/**
	 * Predicate, subject, object
	 */
	PSO(TripleComponentRole.PREDICATE, TripleComponentRole.SUBJECT, TripleComponentRole.OBJECT, 1 << 2),
	/**
	 * Predicate, object, subject
	 */
	POS(TripleComponentRole.PREDICATE, TripleComponentRole.OBJECT, TripleComponentRole.SUBJECT, 1 << 3),
	/**
	 * Object, subject, predicate
	 */
	OSP(TripleComponentRole.OBJECT, TripleComponentRole.SUBJECT, TripleComponentRole.PREDICATE, 1 << 4),
	/**
	 * Object, predicate, subject
	 */
	OPS(TripleComponentRole.OBJECT, TripleComponentRole.PREDICATE, TripleComponentRole.SUBJECT, 1 << 5);

	public static final int ALL_MASK;

	static {
		int allMask = 0;
		// add all the mask to the var
		for (TripleComponentOrder order : values()) {
			allMask |= order.mask;
		}
		ALL_MASK = allMask;
	}

	private final TripleComponentRole subjectMapping;
	private final TripleComponentRole predicateMapping;
	private final TripleComponentRole objectMapping;
	private final TripleComponentRole subjectLookup;
	private final TripleComponentRole predicateLookup;
	private final TripleComponentRole objectLookup;
	public final int mask;

	TripleComponentOrder(TripleComponentRole subjectMapping, TripleComponentRole predicateMapping,
			TripleComponentRole objectMapping, int mask) {
		this.subjectMapping = subjectMapping;
		this.predicateMapping = predicateMapping;
		this.objectMapping = objectMapping;
		this.mask = mask;
		subjectLookup = computeLookup(TripleComponentRole.SUBJECT);
		predicateLookup = computeLookup(TripleComponentRole.PREDICATE);
		objectLookup = computeLookup(TripleComponentRole.OBJECT);
	}

	private TripleComponentRole computeLookup(TripleComponentRole searched) {
		if (searched == subjectMapping) {
			return TripleComponentRole.SUBJECT;
		}
		if (searched == predicateMapping) {
			return TripleComponentRole.PREDICATE;
		}
		if (searched == objectMapping) {
			return TripleComponentRole.OBJECT;
		}
		// unknown
		if (mask == 0) {
			return null;
		}
		throw new IllegalArgumentException("Invalid lookup build : " + searched + " " + this);
	}

	/**
	 * Search for an acceptable value in a map of orders
	 *
	 * @param flags flags to search the value
	 * @param map   map
	 * @param <T>   value type
	 * @return find value, null for no matching value
	 */
	public static <T, Z extends TripleComponentOrder> List<Z> fetchAllBestForCfg(int flags, Map<Z, T> map) {
		ArrayList<Z> ret = new ArrayList<>();
		for (Map.Entry<Z, T> e : map.entrySet()) {
			if ((e.getKey().mask & flags) != 0) {
				ret.add(e.getKey());
			}
		}
		return ret;
	}

	/**
	 * Search for an acceptable value in a map of orders
	 *
	 * @param flags flags to search the value
	 * @param map   map
	 * @param <T>   value type
	 * @return find value, null for no matching value
	 */
	public static <T> T fetchBestForCfg(int flags, Map<? extends TripleComponentOrder, T> map) {
		for (Map.Entry<? extends TripleComponentOrder, T> e : map.entrySet()) {
			if ((e.getKey().mask & flags) != 0) {
				return e.getValue();
			}
		}
		return null;
	}

	/**
	 * get an acceptable order for a order mask
	 *
	 * @param flags order mask
	 * @return order, {@link #Unknown} if nothing was found
	 */
	public static TripleComponentOrder getAcceptableOrder(int flags) {
		if (flags != 0) {
			for (TripleComponentOrder v : values()) {
				if ((v.mask & flags) == 0) {
					return v;
				}
			}
		}
		return Unknown;
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

	public TripleComponentRole getSubjectLookup() {
		return subjectLookup;
	}

	public TripleComponentRole getPredicateLookup() {
		return predicateLookup;
	}

	public TripleComponentRole getObjectLookup() {
		return objectLookup;
	}

	public static ToLongFunction<TripleID> getFunc(TripleComponentRole role) {
		return switch (role) {
		case SUBJECT -> TripleID::getSubject;
		case PREDICATE -> TripleID::getPredicate;
		case OBJECT -> TripleID::getObject;
		case GRAPH -> TripleID::getGraph;
		};
	}

	public static void setRole(TripleComponentRole role, TripleID id, long val) {
		switch (role) {
		case SUBJECT -> id.setSubject(val);
		case PREDICATE -> id.setPredicate(val);
		case OBJECT -> id.setObject(val);
		case GRAPH -> id.setGraph(val);
		}
		;
	}

	public static long getRole(TripleComponentRole role, TripleID id) {
		return switch (role) {
		case SUBJECT -> id.getSubject();
		case PREDICATE -> id.getPredicate();
		case OBJECT -> id.getObject();
		case GRAPH -> id.getGraph();
		};
	}

	public TripleComponentRole getLookup(TripleComponentRole role) {
		return switch (role) {
		case SUBJECT -> getSubjectLookup();
		case PREDICATE -> getPredicateLookup();
		case OBJECT -> getObjectLookup();
		case GRAPH -> TripleComponentRole.GRAPH;
		};
	}

	public TripleComponentRole getMapping(TripleComponentRole role) {
		return switch (role) {
		case SUBJECT -> getSubjectMapping();
		case PREDICATE -> getPredicateMapping();
		case OBJECT -> getObjectMapping();
		case GRAPH -> TripleComponentRole.GRAPH;
		};
	}

	public ToLongFunction<TripleID> getSubjectFunction() {
		return getFunc(getSubjectMapping());
	}

	public ToLongFunction<TripleID> getPredicateFunction() {
		return getFunc(getPredicateMapping());
	}

	public ToLongFunction<TripleID> getObjectFunction() {
		return getFunc(getObjectMapping());
	}

	public void setSubject(TripleID id, long val) {
		setRole(getSubjectMapping(), id, val);
	}

	public void setPredicate(TripleID id, long val) {
		setRole(getPredicateMapping(), id, val);
	}

	public void setObject(TripleID id, long val) {
		setRole(getObjectMapping(), id, val);
	}

	public void remap(TripleID id, TripleComponentOrder origin) {
		long s = id.getSubject();
		long p = id.getPredicate();
		long o = id.getObject();

		setRole(getMapping(origin.getLookup(TripleComponentRole.SUBJECT)), id, s);
		setRole(getMapping(origin.getLookup(TripleComponentRole.PREDICATE)), id, p);
		setRole(getMapping(origin.getLookup(TripleComponentRole.OBJECT)), id, o);
	}
}

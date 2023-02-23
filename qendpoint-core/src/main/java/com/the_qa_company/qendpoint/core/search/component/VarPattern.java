package com.the_qa_company.qendpoint.core.search.component;

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;

/**
 * 2 variable dupe pattern, X = varname, C = other
 *
 * @author Antoine Willerval
 */
public enum VarPattern {
	/**
	 * full spo
	 */
	CCC((hdt, s, p, o) -> true),
	/**
	 * XPX
	 */
	XCX((hdt, s, p, o) -> s == o),
	/**
	 * SXX
	 */
	CXX((hdt, s, p, o) -> poe(hdt, p, o)),
	/**
	 * XXO
	 */
	XXC((hdt, s, p, o) -> spe(hdt, s, p)),
	/**
	 * XXX
	 */
	XXX((hdt, s, p, o) -> s == o && poe(hdt, p, o));

	private static boolean spe(HDT hdt, long s, long p) {
		return testSectionElementEquals(hdt, TripleComponentRole.SUBJECT, s, TripleComponentRole.PREDICATE, p);
	}

	private static boolean poe(HDT hdt, long p, long o) {
		return testSectionElementEquals(hdt, TripleComponentRole.PREDICATE, p, TripleComponentRole.OBJECT, o);
	}

	private static boolean testSectionElementEquals(HDT hdt, TripleComponentRole roleA, long a,
			TripleComponentRole roleB, long b) {
		return hdt.getDictionary().idToString(a, roleA).equals(hdt.getDictionary().idToString(b, roleB));
	}

	/**
	 * create a pattern from a triple
	 *
	 * @param triple triple
	 * @return pattern
	 */
	public static VarPattern of(HDTComponentTriple triple) {

		HDTComponent object = triple.getObject();
		HDTComponent predicate = triple.getPredicate();
		HDTComponent subject = triple.getSubject();

		// faster than creating a mutable list
		if (subject != null && subject.isVariable()) {
			if (predicate != null && predicate.isVariable()) {
				if (object != null && object.isVariable()) {
					// ???
					if (subject.asVariable().getName().equals(predicate.asVariable().getName())) {
						if (predicate.asVariable().getName().equals(object.asVariable().getName())) {
							return VarPattern.XXX;
						} else {
							return VarPattern.XXC;
						}
					} else {
						if (predicate.asVariable().getName().equals(object.asVariable().getName())) {
							return VarPattern.CXX;
						} else {
							return VarPattern.CCC;
						}
					}
				} else {
					// ??o
					if (subject.asVariable().getName().equals(predicate.asVariable().getName())) {
						return VarPattern.XXC;
					} else {
						return VarPattern.CCC;
					}
				}
			} else {
				if (object != null && object.isVariable()) {
					// ?p?
					if (object.asVariable().getName().equals(subject.asVariable().getName())) {
						return VarPattern.XCX;
					} else {
						return VarPattern.CCC;
					}
				} else {
					return VarPattern.CCC;
				}
			}
		} else {
			if (predicate != null && predicate.isVariable()) {
				if (object != null && object.isVariable()) {
					// s??
					if (predicate.asVariable().getName().equals(object.asVariable().getName())) {
						return VarPattern.CXX;
					} else {
						return VarPattern.CCC;
					}
				} else {
					return VarPattern.CCC;
				}
			} else {
				if (object != null && object.isVariable()) {
					return VarPattern.CCC;
				} else {
					return VarPattern.CCC;
				}
			}
		}
	}

	private final VarPredicate predicate;

	VarPattern(VarPredicate predicate) {
		this.predicate = predicate;
	}

	/**
	 * @return is this pattern contain a duplicated variable
	 */
	public boolean hasDuplicated() {
		return this != CCC;
	}

	/**
	 * test a TP
	 *
	 * @param hdt hdt to run po or so conversion
	 * @param s   sid
	 * @param p   pid
	 * @param o   oid
	 * @return if this pattern match the ids
	 */
	public boolean test(HDT hdt, long s, long p, long o) {
		return predicate.test(hdt, s, p, o);
	}

	@FunctionalInterface
	private interface VarPredicate {
		boolean test(HDT hdt, long s, long p, long o);
	}
}

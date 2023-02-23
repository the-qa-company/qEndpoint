package com.the_qa_company.qendpoint.core.search.component;

import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.List;

/**
 * Triple of components in an HDT query
 *
 * @author Antoine Willerval
 */
public interface HDTComponentTriple {
	HDTComponent getSubject();

	/**
	 * @return predicate component
	 */
	HDTComponent getPredicate();

	/**
	 * @return object component
	 */
	HDTComponent getObject();

	/**
	 * @return all the {@link HDTVariable} of the triple
	 */
	default List<HDTVariable> vars() {
		HDTComponent object = getObject();
		HDTComponent predicate = getPredicate();
		HDTComponent subject = getSubject();

		// faster than creating a mutable list
		if (subject != null && subject.isVariable()) {
			if (predicate != null && predicate.isVariable()) {
				if (object != null && object.isVariable()) {
					return List.of(subject.asVariable(), predicate.asVariable(), object.asVariable());
				} else {
					return List.of(subject.asVariable(), predicate.asVariable());
				}
			} else {
				if (object != null && object.isVariable()) {
					return List.of(subject.asVariable(), object.asVariable());
				} else {
					return List.of(subject.asVariable());
				}

			}
		} else {
			if (predicate != null && predicate.isVariable()) {
				if (object != null && object.isVariable()) {
					return List.of(predicate.asVariable(), object.asVariable());
				} else {
					return List.of(predicate.asVariable());
				}
			} else {
				if (object != null && object.isVariable()) {
					return List.of(object.asVariable());
				} else {
					return List.of();
				}
			}
		}
	}

	/**
	 * @return pattern of the component {@literal "spo"} for no var
	 *         {@literal "???"} for full vars
	 */
	default String pattern() {
		HDTComponent object = getObject();
		HDTComponent predicate = getPredicate();
		HDTComponent subject = getSubject();

		// faster than creating a mutable list
		if (subject != null && subject.isVariable()) {
			if (predicate != null && predicate.isVariable()) {
				if (object != null && object.isVariable()) {
					return "???";
				} else {
					return "??o";
				}
			} else {
				if (object != null && object.isVariable()) {
					return "?p?";
				} else {
					return "?po";
				}
			}
		} else {
			if (predicate != null && predicate.isVariable()) {
				if (object != null && object.isVariable()) {
					return "s??";
				} else {
					return "s?o";
				}
			} else {
				if (object != null && object.isVariable()) {
					return "sp?";
				} else {
					return "spo";
				}
			}
		}
	}

	/**
	 * @return this triple as a {@link TripleID}
	 */
	default TripleID asTripleId() {
		HDTComponent s = getSubject();
		HDTComponent p = getPredicate();
		HDTComponent o = getObject();

		long sid;
		long pid;
		long oid;

		if (s == null || s.isVariable()) {
			sid = 0;
		} else {
			sid = s.asConstant().getAsSubjectId();
		}

		if (p == null || p.isVariable()) {
			pid = 0;
		} else {
			pid = p.asConstant().getAsPredicate();
		}

		if (o == null || o.isVariable()) {
			oid = 0;
		} else {
			oid = o.asConstant().getAsObjectId();
		}

		return new TripleID(sid, pid, oid);
	}
}

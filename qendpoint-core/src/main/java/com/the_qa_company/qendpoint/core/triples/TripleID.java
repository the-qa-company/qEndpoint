/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/
 * triples/TripleID.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

package com.the_qa_company.qendpoint.core.triples;

import java.io.Serializable;

import com.the_qa_company.qendpoint.core.util.LongCompare;

/**
 * TripleID holds a triple using Long IDs
 */
public final class TripleID implements Comparable<TripleID>, Serializable, Cloneable {
	private static final long serialVersionUID = -4685524566493494912L;

	private long subject;
	private long predicate;
	private long object;

	/**
	 * Basic constructor
	 */
	public TripleID() {
		super();
	}

	/**
	 * Constructor
	 *
	 * @param subject   The subject
	 * @param predicate The predicate
	 * @param object    The object
	 */
	public TripleID(long subject, long predicate, long object) {
		super();
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	/**
	 * Build a TripleID as a copy of another one.
	 *
	 * @param other the triple ID to copy
	 */
	public TripleID(TripleID other) {
		super();
		this.subject = other.subject;
		this.predicate = other.predicate;
		this.object = other.object;
	}

	/**
	 * @return long the subject
	 */
	public long getSubject() {
		return subject;
	}

	/**
	 * @param subject the subject to set
	 */
	public void setSubject(long subject) {
		this.subject = subject;
	}

	/**
	 * @return long the object
	 */
	public long getObject() {
		return object;
	}

	/**
	 * @param object the object to set
	 */
	public void setObject(long object) {
		this.object = object;
	}

	/**
	 * @return long the predicate
	 */
	public long getPredicate() {
		return predicate;
	}

	/**
	 * @param predicate the predicate to set
	 */
	public void setPredicate(long predicate) {
		this.predicate = predicate;
	}

	/**
	 * Replace all components of a TripleID at once. Useful to reuse existing
	 * objects.
	 *
	 * @param subject   subject ID
	 * @param predicate predicate ID
	 * @param object    object ID
	 */
	public void setAll(long subject, long predicate, long object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	public void assign(TripleID replacement) {
		subject = replacement.getSubject();
		object = replacement.getObject();
		predicate = replacement.getPredicate();
	}

	/**
	 * Set all components to zero.
	 */
	public void clear() {
		subject = predicate = object = 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return subject + " " + predicate + " " + object;
	}

	/**
	 * Compare TripleID to another one using SPO Order. To compare using other
	 * orders use @see
	 * com.the_qa_company.qendpoint.core.triples.TripleStringComparator
	 */
	@Override
	public int compareTo(TripleID other) {
		int result = LongCompare.compare(this.subject, other.subject);

		if (result == 0) {
			result = LongCompare.compare(this.predicate, other.predicate);
			if (result == 0) {
				return LongCompare.compare(this.object, other.object);
			} else {
				return result;
			}
		} else {
			return result;
		}
	}

	/**
	 * Check whether this triple matches a pattern of TripleID. 0 acts as a
	 * wildcard
	 *
	 * @param pattern The pattern to match against
	 * @return boolean
	 */
	public boolean match(TripleID pattern) {

		// get the components of the pattern
		long subjectPattern = pattern.getSubject();
		long predicatePattern = pattern.getPredicate();
		long objectPattern = pattern.getObject();

		/* Remember that 0 acts as a wildcard */
		if (subjectPattern == 0 || this.subject == subjectPattern) {
			if (predicatePattern == 0 || this.predicate == predicatePattern) {
				return objectPattern == 0 || this.object == objectPattern;
			}
		}
		return false;
	}

	/**
	 * Check whether all the components of the triple are empty (zero).
	 *
	 * @return boolean
	 */
	public boolean isEmpty() {
		return !(subject != 0 || predicate != 0 || object != 0);
	}

	/**
	 * Check whether none of the components of the triple are empty.
	 *
	 * @return boolean
	 */
	public boolean isValid() {
		return subject > 0 && predicate > 0 && object > 0;
	}

	/**
	 * Checks whether any of the components of the triple are "no match" (-1).
	 *
	 * @return boolean
	 */
	public boolean isNoMatch() {
		return subject == -1 || predicate == -1 || object == -1;
	}

	/**
	 * Get the pattern of the triple as String, such as "SP?".
	 *
	 * @return String
	 */
	public String getPatternString() {
		return "" + (subject == 0 ? '?' : 'S') + (predicate == 0 ? '?' : 'P') + (object == 0 ? '?' : 'O');
	}

	/**
	 * size of one TripleID in memory
	 *
	 * @return int
	 */
	public static int size() {
		return 48;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof TripleID)) {
			return false;
		}
		TripleID other = (TripleID) o;
		return !(subject != other.subject || predicate != other.predicate || object != other.object);
	}

	@Override
	public TripleID clone() {
		try {
			return (TripleID) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new AssertionError(e);
		}
	}

	@Override
	public int hashCode() {
		return (int) (subject * 13 + predicate * 17 + object * 31);
	}
}

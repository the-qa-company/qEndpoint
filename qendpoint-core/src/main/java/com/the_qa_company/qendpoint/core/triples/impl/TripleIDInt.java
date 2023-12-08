/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/triples/TripleID.java
 * $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom,
 * 03 mar 2013) $ Last modified by: $Author: mario.arias $ This library is free
 * software; you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License. This library is distributed in the hope that it
 * will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.triples.TripleID;

/**
 * TripleID holds a triple as integers
 */
public final class TripleIDInt implements Comparable<TripleIDInt> {

	private int subject;
	private int predicate;
	private int object;
	private int graph;
	private boolean isQuad = false;

	/**
	 * Basic constructor
	 */
	public TripleIDInt() {
	}

	/**
	 * Constructor
	 *
	 * @param subject   The subject
	 * @param predicate The predicate
	 * @param object    The object
	 */
	public TripleIDInt(int subject, int predicate, int object) {
		this();
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	public TripleIDInt(long subject, long predicate, long object) {
		this();
		this.subject = (int) subject;
		this.predicate = (int) predicate;
		this.object = (int) object;
	}

	/**
	 * Constructor
	 *
	 * @param subject   The subject
	 * @param predicate The predicate
	 * @param object    The object
	 * @param graph     The graph
	 */
	public TripleIDInt(int subject, int predicate, int object, int graph) {
		this(subject, predicate, object);
		this.graph = graph;
		this.isQuad = true;
	}

	/**
	 * Constructor
	 *
	 * @param subject   The subject
	 * @param predicate The predicate
	 * @param object    The object
	 * @param graph     The graph
	 */
	public TripleIDInt(long subject, long predicate, long object, long graph) {
		this(subject, predicate, object);
		this.graph = (int) graph;
		this.isQuad = true;
	}

	/**
	 * Build a TripleID as a copy of another one.
	 *
	 * @param other other
	 */
	public TripleIDInt(TripleIDInt other) {
		this.subject = other.subject;
		this.predicate = other.predicate;
		this.object = other.object;
		this.graph = other.graph;
		this.isQuad = other.isQuad;
	}

	public TripleIDInt(TripleID other) {
		this.subject = (int) other.getSubject();
		this.predicate = (int) other.getPredicate();
		this.object = (int) other.getObject();
		this.graph = (int) other.getGraph();
		this.isQuad = other.isQuad();
	}

	/**
	 * @return the subject
	 */
	public int getSubject() {
		return subject;
	}

	/**
	 * @param subject the subject to set
	 */
	public void setSubject(int subject) {
		this.subject = subject;
	}

	/**
	 * @return the object
	 */
	public int getObject() {
		return object;
	}

	/**
	 * @param object the object to set
	 */
	public void setObject(int object) {
		this.object = object;
	}

	/**
	 * @return the predicate
	 */
	public int getPredicate() {
		return predicate;
	}

	/**
	 * @param predicate the predicate to set
	 */
	public void setPredicate(int predicate) {
		this.predicate = predicate;
	}

	/**
	 * @return the graph
	 */
	public int getGraph() {
		return graph;
	}

	/**
	 * @param graph the graph to set
	 */
	public void setGraph(int graph) {
		this.graph = graph;
	}

	/**
	 * Replace all components of a TripleID at once. Useful to reuse existing
	 * objects.
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 */
	public void setAll(int subject, int predicate, int object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.isQuad = false;
	}

	/**
	 * Replace all components of a TripleID at once. Useful to reuse existing
	 * objects.
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @param graph     graph
	 */
	public void setAll(int subject, int predicate, int object, int graph) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.graph = graph;
		this.isQuad = true;
	}

	public void assign(TripleIDInt replacement) {
		subject = replacement.getSubject();
		object = replacement.getObject();
		predicate = replacement.getPredicate();
		graph = replacement.getGraph();
		isQuad = replacement.isQuad();
	}

	public boolean isQuad() {
		return isQuad;
	}

	/**
	 * Set all components to zero.
	 */
	public void clear() {
		subject = predicate = object = graph = 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (isQuad) {
			return subject + " " + predicate + " " + object + " " + graph;
		}
		return subject + " " + predicate + " " + object;
	}

	public boolean equals(TripleIDInt other) {
		return !(subject != other.subject || predicate != other.predicate || object != other.object
				|| graph != other.graph);
	}

	/**
	 * Compare TripleID to another one using SPO Order. To compare using other
	 * orders use @see TripleStringComparator
	 */
	@Override
	public int compareTo(TripleIDInt other) {
		int result = this.subject - other.subject;

		if (result == 0) {
			result = this.predicate - other.predicate;
			if (result == 0) {
				result = this.object - other.object;
				if (result == 0) {
					return this.graph - other.graph;
				} else {
					return result;
				}
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
		long graphPattern = pattern.getGraph();

		// Remember that 0 acts as a wildcard
		if (subjectPattern == 0 || this.subject == subjectPattern) {
			if (predicatePattern == 0 || this.predicate == predicatePattern) {
				if (objectPattern == 0 || this.object == objectPattern) {
					return graphPattern == 0 || this.graph == graphPattern;
				}
			}
		}
		return false;
	}

	/**
	 * Check whether all the components of the triple are empty (zero).
	 */
	public boolean isEmpty() {
		return !(subject != 0 || predicate != 0 || object != 0 || graph != 0);
	}

	/**
	 * Check whether none of the components of the triple are empty.
	 */
	public boolean isValid() {
		return subject > 0 && predicate > 0 && object > 0 && (!isQuad || graph > 0);
	}

	/**
	 * Get the pattern of the triple as String, such as "SP?".
	 */
	public String getPatternString() {
		return String.valueOf(subject == 0 ? '?' : 'S') + (predicate == 0 ? '?' : 'P') + (object == 0 ? '?' : 'O')
				+ (isQuad ? (graph == 0 ? '?' : 'G') : "");
	}

	public TripleID asTripleID() {
		if (isQuad) {
			return new TripleID(subject, predicate, object, graph);
		}
		return new TripleID(subject, predicate, object);
	}

	/**
	 * size of one TripleID in memory
	 */
	public static int size() {
		return 33;
	}

}

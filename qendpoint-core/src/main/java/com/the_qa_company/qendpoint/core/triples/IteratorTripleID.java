/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/
 * triples/IteratorTripleID.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;

import java.util.Iterator;

/**
 * Iterator of TripleID
 */
public interface IteratorTripleID extends Iterator<TripleID> {

	/**
	 * Whether the iterator has previous elements.
	 *
	 * @return boolean
	 */
	boolean hasPrevious();

	/**
	 * Get the previous element. Call only if hasPrevious() returns true. It
	 * moves the cursor of the Iterator to the previous entry.
	 *
	 * @return TripleID
	 */
	TripleID previous();

	/**
	 * Point the cursor to the first element of the data structure.
	 */
	void goToStart();

	/**
	 * Specifies whether the iterator can move to a random position.
	 *
	 * @return boolean
	 */
	boolean canGoTo();

	/**
	 * Go to the specified random position. Only use whenever canGoTo() returns
	 * true.
	 *
	 * @param pos got to a given position
	 */
	void goTo(long pos);

	/**
	 * Returns the number of estimated results of the Iterator. It is usually
	 * more efficient than going through all the results.
	 *
	 * @return long Number of estimated results.
	 */
	long estimatedNumResults();

	/**
	 * Returns the accuracy of the estimation of number of results as returned
	 * by estimatedNumResults()
	 *
	 * @return ResultEstimationType
	 */
	ResultEstimationType numResultEstimation();

	/**
	 * Return the order in which the triples are iterated (Might be unknown)
	 *
	 * @return TripleComponentOrder
	 */
	TripleComponentOrder getOrder();

	/**
	 * Return the position of the triple of the last next call, from 1 to
	 * numTriples.
	 *
	 * @return position
	 * @see Triples#findTriple(long)
	 */
	long getLastTriplePosition();

	/**
	 * @return if {@link #jumpToSubject(long)} can be used
	 */
	default boolean canJumpToSubject() {
		return false;
	}

	/**
	 * @return if {@link #jumpToPredicate(long)} can be used
	 */
	default boolean canJumpToPredicate() {
		return false;
	}

	/**
	 * @return if {@link #jumpToObject(long)} can be used
	 */
	default boolean canJumpToObject() {
		return false;
	}

	/**
	 * jump to a subject, if the subject can't be found to the next element.
	 * This function doesn't do anything if {@link #canJumpToSubject()} returns
	 * false
	 *
	 * @param id id to jump
	 */
	default void jumpToSubject(long id) {
	}

	/**
	 * jump to a predicate, if the predicate can't be found to the next element.
	 * This function doesn't do anything if {@link #canJumpToPredicate()}
	 * returns false
	 *
	 * @param id id to jump
	 */
	default void jumpToPredicate(long id) {
	}

	/**
	 * jump to an object, if the object can't be found to the next element. This
	 * function doesn't do anything if {@link #canJumpToObject()} returns false
	 *
	 * @param id id to jump
	 */
	default void jumpToObject(long id) {
	}

	/**
	 * @return if the {@link #getLastTriplePosition()} function is returning an
	 *         index based on {@link #getOrder()} or the triples order
	 */
	default boolean isLastTriplePositionBoundToOrder() {
		return false;
	}
}

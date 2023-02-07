/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * iterator/SequentialSearchIteratorTripleID.java $ Revision: $Rev: 196 $ Last
 * modified: $Date: 2013-04-12 10:34:20 +0100 (vie, 12 abr 2013) $ Last modified
 * by: $Author: mario.arias $ This library is free software; you can
 * redistribute it and/or modify it under the terms of the GNU Lesser General
 * Public License as published by the Free Software Foundation; version 3.0 of
 * the License. This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.iterator;

import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.TripleID;

/**
 * Given an iterator of TripleID's, provides a new iterator that filters only
 * triples that match the supplied pattern.
 *
 * @author mario.arias
 */
public class SequentialSearchIteratorTripleID implements SuppliableIteratorTripleID {
	private final TripleID pattern;
	private final TripleID nextTriple;
	private TriplePositionSupplier nextPosition;
	private TriplePositionSupplier lastPosition;
	private TriplePositionSupplier previousPosition;
	private TripleID previousTriple;
	private final TripleID returnTriple;
	final SuppliableIteratorTripleID iterator;
	boolean hasMoreTriples, hasPreviousTriples;
	boolean goingUp;

	public SequentialSearchIteratorTripleID(TripleID pattern, SuppliableIteratorTripleID other) {
		this.pattern = pattern;
		this.iterator = other;
		hasPreviousTriples = false;
		goingUp = true;
		nextTriple = new TripleID();
		returnTriple = new TripleID();
		doFetchNext();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return hasMoreTriples;
	}

	private void doFetchNext() {
		hasMoreTriples = false;

		while (iterator.hasNext()) {
			TripleID next = iterator.next();

			if (next.match(pattern)) {
				hasMoreTriples = true;
				hasPreviousTriples = true;
				nextTriple.assign(next);
				nextPosition = iterator.getLastTriplePositionSupplier();
				break;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#next()
	 */
	@Override
	public TripleID next() {
		if (!goingUp) {
			goingUp = true;
			if (hasPreviousTriples) {
				doFetchNext();
			}
			doFetchNext();
		}
		returnTriple.assign(nextTriple);
		lastPosition = nextPosition;

		doFetchNext();

		return returnTriple;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasPrevious()
	 */
	@Override
	public boolean hasPrevious() {
		return hasPreviousTriples;
	}

	private void doFetchPrevious() {
		hasPreviousTriples = false;

		while (iterator.hasPrevious()) {
			TripleID previous = iterator.previous();

			if (previous.match(pattern)) {
				hasPreviousTriples = true;
				hasMoreTriples = true;
				previousTriple.assign(previous);
				previousPosition = iterator.getLastTriplePositionSupplier();
				break;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#previous()
	 */
	@Override
	public TripleID previous() {
		if (goingUp) {
			goingUp = false;
			if (hasMoreTriples) {
				doFetchPrevious();
			}
			doFetchPrevious();
		}
		returnTriple.assign(previousTriple);
		lastPosition = previousPosition;

		doFetchPrevious();

		return returnTriple;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#goToStart()
	 */
	@Override
	public void goToStart() {
		iterator.goToStart();
		doFetchNext();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#estimatedNumResults()
	 */
	@Override
	public long estimatedNumResults() {
		return iterator.estimatedNumResults();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#numResultEstimation()
	 */
	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.UP_TO;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#canGoTo()
	 */
	@Override
	public boolean canGoTo() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#goTo(int)
	 */
	@Override
	public void goTo(long pos) {
		throw new IllegalArgumentException("Called goTo() on an unsupported implementation");
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#getOrder()
	 */
	@Override
	public TripleComponentOrder getOrder() {
		return iterator.getOrder();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getLastTriplePosition() {
		return lastPosition.compute();
	}

	@Override
	public TriplePositionSupplier getLastTriplePositionSupplier() {
		return lastPosition;
	}
}

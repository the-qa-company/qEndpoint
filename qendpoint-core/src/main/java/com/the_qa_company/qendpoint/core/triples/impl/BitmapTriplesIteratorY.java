/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples
 * /impl/BitmapTriplesIteratorY.java $ Revision: $Rev: 191 $ Last modified:
 * $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by:
 * $Author: mario.arias $ This library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.AdjacencyList;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.iterator.TriplePositionSupplier;
import com.the_qa_company.qendpoint.core.triples.TripleID;

/**
 * Iterates over all Y components of a BitmapTriples. i.e. In SPO it would
 * iterate over all appearances of a predicate ?P?
 *
 * @author mario.arias
 */
public class BitmapTriplesIteratorY implements SuppliableIteratorTripleID {

	private final BitmapTriples triples;
	private long lastPosZ, lastNextZ, lastNextY;
	private final TripleID returnTriple;
	private final long patY;

	private final AdjacencyList adjY;
	private final AdjacencyList adjZ;
	long posY, posZ;
	private long prevY, nextY, prevZ, nextZ;
	private long x, y, z;

	public BitmapTriplesIteratorY(BitmapTriples triples, TripleID pattern) {
		this.triples = triples;
		TripleID pattern1 = new TripleID(pattern);
		this.returnTriple = new TripleID();

		TripleOrderConvert.swapComponentOrder(pattern1, TripleComponentOrder.SPO, triples.order);
		patY = pattern1.getPredicate();
		if (patY == 0) {
			throw new IllegalArgumentException("This structure is not meant to process this pattern");
		}

		adjY = triples.adjY;
		adjZ = triples.adjZ;

		goToStart();
	}

	private void updateOutput() {
		lastPosZ = posZ;
		lastNextZ = nextZ;
		lastNextY = nextY;
		returnTriple.setAll(x, y, z);
		TripleOrderConvert.swapComponentOrder(returnTriple, triples.order, TripleComponentOrder.SPO);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return nextY != -1 || posZ <= nextZ;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#next()
	 */
	@Override
	public TripleID next() {
		if (posZ > nextZ) {
			prevY = posY;
			posY = nextY;
			nextY = adjY.findNextAppearance(nextY + 1, patY);

			posZ = prevZ = adjZ.find(posY);
			nextZ = adjZ.last(posY);

			x = adjY.findListIndex(posY) + 1;
			y = adjY.get(posY);
			z = adjZ.get(posZ);
		} else {
			z = adjZ.get(posZ);
		}

		updateOutput();

		posZ++;

		return returnTriple;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasPrevious()
	 */
	@Override
	public boolean hasPrevious() {
		return prevY != -1 || posZ >= prevZ;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#previous()
	 */
	@Override
	public TripleID previous() {
		if (posZ <= prevZ) {
			nextY = posY;
			posY = prevY;
			prevY = adjY.findPreviousAppearance(prevY - 1, patY);

			posZ = prevZ = adjZ.find(posY);
			nextZ = adjZ.last(posY);

			x = adjY.findListIndex(posY) + 1;
			y = adjY.get(posY);
			z = adjZ.get(posZ);
		} else {
			posZ--;
			z = adjZ.get(posZ);
		}

		updateOutput();

		return returnTriple;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#goToStart()
	 */
	@Override
	public void goToStart() {
		prevY = -1;
		posY = adjY.findNextAppearance(0, patY);
		nextY = adjY.findNextAppearance(posY + 1, patY);

		posZ = prevZ = adjZ.find(posY);
		nextZ = adjZ.last(posY);

		x = adjY.findListIndex(posY) + 1;
		y = adjY.get(posY);
		z = adjZ.get(posZ);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#estimatedNumResults()
	 */
	@Override
	public long estimatedNumResults() {
		return adjZ.getNumberOfElements();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#numResultEstimation()
	 */
	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.UNKNOWN;
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
		if (!canGoTo()) {
			throw new IllegalAccessError("Cannot goto on this bitmaptriples pattern");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#getOrder()
	 */
	@Override
	public TripleComponentOrder getOrder() {
		return triples.order;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	private long computeLastTriplePos(long posZ, long nextZ, long nextY) {
		if (posZ > nextZ)
			return adjZ.find(nextY);
		else
			return posZ;
	}

	@Override
	public long getLastTriplePosition() {
		return computeLastTriplePos(lastPosZ, lastNextZ, lastNextY);
	}

	@Override
	public TriplePositionSupplier getLastTriplePositionSupplier() {
		final long flastPosZ = lastPosZ, flastNextZ = lastNextZ, flastNextY = lastNextY;
		return () -> computeLastTriplePos(flastPosZ, flastNextZ, flastNextY);
	}
}

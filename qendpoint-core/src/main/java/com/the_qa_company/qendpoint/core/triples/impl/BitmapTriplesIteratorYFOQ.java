/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples/impl/BitmapTriplesIteratorYFOQ.java
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

import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.compact.bitmap.AdjacencyList;

/**
 * Iterates over all Y components of a BitmapTriples. i.e. In SPO it would
 * iterate over all appearances of a predicate ?P?
 *
 * @author mario.arias
 */
public class BitmapTriplesIteratorYFOQ implements SuppliableIteratorTripleID {
	private final BitmapTriples triples;
	private long lastPosition;
	private final TripleID returnTriple;
	private final long patY;

	private final AdjacencyList adjY;
	private final AdjacencyList adjZ;
	private long posY;
	long posZ;
	private long prevZ;
	private long nextZ;
	private final long maxZ;
	private long x, y, z;

	private final long numOccurrences;
	private long numOccurrence;
	private final long predBase;

	public BitmapTriplesIteratorYFOQ(BitmapTriples triples, TripleID pattern) {
		this.triples = triples;
		TripleID pattern1 = new TripleID(pattern);
		this.returnTriple = new TripleID();

		TripleOrderConvert.swapComponentOrder(pattern1, TripleComponentOrder.SPO, triples.order);
		patY = pattern1.getPredicate();
		if (patY == 0) {
			throw new IllegalArgumentException("This structure is not meant to process this pattern");
		}

		adjY = new AdjacencyList(triples.getSeqY(), triples.getBitmapY());
		adjZ = new AdjacencyList(triples.getSeqZ(), triples.getBitmapZ());

		numOccurrences = triples.predicateIndex.getNumOcurrences(patY);
		predBase = triples.predicateIndex.getBase(patY);
		maxZ = triples.adjZ.getNumberOfElements();

		goToStart();
	}

	private void updateOutput() {
		lastPosition = posZ;
		returnTriple.setAll(x, y, z);
		TripleOrderConvert.swapComponentOrder(returnTriple, triples.order, TripleComponentOrder.SPO);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return posZ < maxZ && (numOccurrence < numOccurrences) || posZ <= nextZ;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#next()
	 */
	@Override
	public TripleID next() {
		if (posZ > nextZ) {
			numOccurrence++;
			posY = triples.predicateIndex.getOccurrence(predBase, numOccurrence);

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
		return numOccurrence > 1 || posZ >= prevZ;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#previous()
	 */
	@Override
	public TripleID previous() {
		if (posZ <= prevZ) {
			numOccurrence--;
			posY = triples.predicateIndex.getOccurrence(predBase, numOccurrence);

			prevZ = adjZ.find(posY);
			posZ = nextZ = adjZ.last(posY);

			x = adjY.findListIndex(posY) + 1;
			y = adjY.get(posY);
			z = adjZ.get(posZ);
		} else {
			z = adjZ.get(posZ);
			posZ--;
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
		numOccurrence = 1;
		posY = triples.predicateIndex.getOccurrence(predBase, numOccurrence);

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
		return triples.predicateCount.get(patY - 1);
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
		return TripleComponentOrder.Unknown; // triples.order;
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
		return lastPosition;
	}

	public long getPosY() {
		return posY;
	}

	public long getPosZ() {
		return posZ;
	}
}

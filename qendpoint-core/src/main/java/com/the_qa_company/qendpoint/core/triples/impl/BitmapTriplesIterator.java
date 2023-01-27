/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples
 * /impl/BitmapTriplesIterator.java $ Revision: $Rev: 191 $ Last modified:
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

import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.compact.bitmap.AdjacencyList;

/**
 * @author mario.arias
 */
public class BitmapTriplesIterator implements SuppliableIteratorTripleID {

	private final BitmapTriples triples;
	private final TripleID pattern, returnTriple;
	private long lastPosition;
	private long patX, patY, patZ;

	private AdjacencyList adjY, adjZ;
	long posY, posZ, minY, minZ, maxY, maxZ;
	private long nextY, nextZ;
	private long x, y, z;

	public BitmapTriplesIterator(BitmapTriples triples, TripleID pattern) {
		this.triples = triples;
		this.returnTriple = new TripleID();
		this.pattern = new TripleID();
		newSearch(pattern);
	}

	public BitmapTriplesIterator(BitmapTriples triples, long minZ, long maxZ) {
		this.triples = triples;
		this.returnTriple = new TripleID();
		this.pattern = new TripleID();
		adjY = triples.adjY;
		adjZ = triples.adjZ;

		this.minZ = minZ;
		this.maxZ = maxZ;
		this.minY = adjZ.findListIndex(minZ);
		this.maxY = adjZ.findListIndex(maxZ);
		goToStart();
	}

	public void newSearch(TripleID pattern) {
		this.pattern.assign(pattern);

		TripleOrderConvert.swapComponentOrder(this.pattern, TripleComponentOrder.SPO, triples.order);
		patX = this.pattern.getSubject();
		patY = this.pattern.getPredicate();
		patZ = this.pattern.getObject();

		adjY = triples.adjY;
		adjZ = triples.adjZ;

		// ((BitSequence375)triples.bitmapZ).dump();

		findRange();
		goToStart();
	}

	private void updateOutput() {
		lastPosition = posZ;
		returnTriple.setAll(x, y, z);
		TripleOrderConvert.swapComponentOrder(returnTriple, triples.order, TripleComponentOrder.SPO);
	}

	private void findRange() {
		if (patX != 0) {
			// S X X
			if (patY != 0) {
				minY = adjY.find(patX - 1, patY);
				if (minY == -1) {
					minY = minZ = maxY = maxZ = 0;
				} else {
					maxY = minY + 1;
					if (patZ != 0) {
						// S P O
						minZ = adjZ.find(minY, patZ);
						if (minZ == -1) {
							minY = minZ = maxY = maxZ = 0;
						} else {
							maxZ = minZ + 1;

						}
					} else {
						// S P ?
						minZ = adjZ.find(minY);
						maxZ = adjZ.last(minY) + 1;
					}
				}

			} else {
				// S ? X
				minY = adjY.find(patX - 1);
				minZ = adjZ.find(minY);
				maxY = adjY.last(patX - 1) + 1;
				maxZ = adjZ.find(maxY);
			}
			x = patX;
		} else {
			// ? X X
			minY = 0;
			minZ = 0;
			maxY = adjY.getNumberOfElements();
			maxZ = adjZ.getNumberOfElements();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return posZ < maxZ;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#next()
	 */
	@Override
	public TripleID next() {
		z = adjZ.get(posZ);
		if (posZ == nextZ) {
			posY++;
			y = adjY.get(posY);
//			nextZ = adjZ.find(posY+1);
			nextZ = adjZ.findNext(nextZ) + 1;

			if (posY == nextY) {
				x++;
				// nextY = adjY.find(x);
				nextY = adjY.findNext(nextY) + 1;
			}
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
		return posZ > minZ;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#previous()
	 */
	@Override
	public TripleID previous() {
		posZ--;

		posY = adjZ.findListIndex(posZ);

		z = adjZ.get(posZ);
		y = adjY.get(posY);
		x = adjY.findListIndex(posY) + 1;

		nextY = adjY.last(x - 1) + 1;
		nextZ = adjZ.last(posY) + 1;

		updateOutput();

		return returnTriple;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#goToStart()
	 */
	@Override
	public void goToStart() {
		posZ = minZ;
		posY = adjZ.findListIndex(posZ);

		z = adjZ.get(posZ);
		y = adjY.get(posY);
		x = adjY.findListIndex(posY) + 1;

		nextY = adjY.last(x - 1) + 1;
		nextZ = adjZ.last(posY) + 1;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#estimatedNumResults()
	 */
	@Override
	public long estimatedNumResults() {
		return maxZ - minZ;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#numResultEstimation()
	 */
	@Override
	public ResultEstimationType numResultEstimation() {
		if (patX != 0 && patY == 0 && patZ != 0) {
			return ResultEstimationType.UP_TO;
		}
		return ResultEstimationType.EXACT;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#canGoTo()
	 */
	@Override
	public boolean canGoTo() {
		return pattern.isEmpty();
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

		if (pos >= adjZ.getNumberOfElements()) {
			throw new ArrayIndexOutOfBoundsException("Cannot goTo beyond last triple");
		}

		posZ = pos;
		posY = adjZ.findListIndex(posZ);

		z = adjZ.get(posZ);
		y = adjY.get(posY);
		x = adjY.findListIndex(posY) + 1;

		nextY = adjY.last(x - 1) + 1;
		nextZ = adjZ.last(posY) + 1;
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

	@Override
	public long getLastTriplePosition() {
		return lastPosition;
	}
}

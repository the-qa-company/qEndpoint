/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples
 * /impl/BitmapTriplesIteratorZFOQ.java $ Revision: $Rev: 191 $ Last modified:
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
import com.the_qa_company.qendpoint.core.iterator.TriplePositionSupplier;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.compact.bitmap.AdjacencyList;

/**
 * @author mario.arias
 */
public class BitmapTriplesIteratorZFOQ implements SuppliableIteratorTripleID {
	private long lastPosIndex;
	protected final BitmapTriples triples;
	protected final TripleID pattern;
	protected final TripleID returnTriple;

	protected AdjacencyList adjY, adjIndex;
	protected long posIndex, minIndex, maxIndex;
	protected long x, y, z;

	protected long patY;
	protected long patZ;

	public BitmapTriplesIteratorZFOQ(BitmapTriples triples, TripleID pattern) {
		this.triples = triples;
		this.pattern = new TripleID(pattern);
		this.returnTriple = new TripleID();

		TripleOrderConvert.swapComponentOrder(this.pattern, TripleComponentOrder.SPO, triples.order);
		patZ = this.pattern.getObject();
		if (patZ == 0 && (patY != 0 || this.pattern.getSubject() != 0)) {
			throw new IllegalArgumentException("This structure is not meant to process this pattern");
		}

		patY = this.pattern.getPredicate();

		adjY = triples.adjY;
		adjIndex = triples.adjIndex;

		calculateRange();
		goToStart();
	}

	protected long getY(long index) {
		return adjY.get(adjIndex.get(index));
	}

	protected void calculateRange() {
		if (patZ == 0) {
			minIndex = 0;
			maxIndex = adjIndex.getNumberOfElements();
			return;
		}
		minIndex = adjIndex.find(patZ - 1);
		maxIndex = adjIndex.last(patZ - 1);

		if (patY != 0) {
			while (minIndex <= maxIndex) {
				long mid = (minIndex + maxIndex) / 2;
				long predicate = getY(mid);

				if (patY > predicate) {
					minIndex = mid + 1;
				} else if (patY < predicate) {
					maxIndex = mid - 1;
				} else {
					// Binary Search to find left boundary
					long left = minIndex;
					long right = mid;
					long pos = 0;

					while (left <= right) {
						pos = (left + right) / 2;

						predicate = getY(pos);

						if (predicate != patY) {
							left = pos + 1;
						} else {
							right = pos - 1;
						}
					}
					minIndex = predicate == patY ? pos : pos + 1;

					// Binary Search to find right boundary
					left = mid;
					right = maxIndex;

					while (left <= right) {
						pos = (left + right) / 2;
						predicate = getY(pos);

						if (predicate != patY) {
							right = pos - 1;
						} else {
							left = pos + 1;
						}
					}
					maxIndex = predicate == patY ? pos : pos - 1;

					break;
				}
			}
		}
	}

	protected void updateOutput() {
		lastPosIndex = posIndex;
		returnTriple.setAll(x, y, z);
		TripleOrderConvert.swapComponentOrder(returnTriple, triples.order, TripleComponentOrder.SPO);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return posIndex <= maxIndex;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#next()
	 */
	@Override
	public TripleID next() {
		long posY = adjIndex.get(posIndex);

		z = patZ != 0 ? patZ : adjIndex.findListIndex(posIndex) + 1;
		y = patY != 0 ? patY : adjY.get(posY);
		x = adjY.findListIndex(posY) + 1;

		updateOutput();

		posIndex++;

		return returnTriple;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#goToStart()
	 */
	@Override
	public void goToStart() {
		posIndex = minIndex;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#estimatedNumResults()
	 */
	@Override
	public long estimatedNumResults() {
		return maxIndex - minIndex + 1;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#numResultEstimation()
	 */
	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.EXACT;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#canGoTo()
	 */
	@Override
	public boolean canGoTo() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#goTo(long)
	 */
	@Override
	public void goTo(long pos) {
		if (pos > maxIndex - minIndex || pos < 0) {
			throw new IndexOutOfBoundsException();
		}
		posIndex = minIndex + pos;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#getOrder()
	 */
	@Override
	public TripleComponentOrder getOrder() {
		return TripleComponentOrder.Unknown;// triples.order;
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
		return triples.adjZ.find(adjIndex.get(lastPosIndex), patZ);
	}

	@Override
	public TriplePositionSupplier getLastTriplePositionSupplier() {
		final long flastPosIndex = this.lastPosIndex;
		return () -> triples.adjZ.find(adjIndex.get(flastPosIndex), patZ);
	}
}

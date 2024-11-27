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
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.compact.bitmap.AdjacencyList;

/**
 * @author mario.arias
 */
public class BitmapTriplesIterator implements SuppliableIteratorTripleID {

	protected final BitmapTriplesIndex idx;
	protected final TripleID pattern, returnTriple;
	protected long lastPosition;
	protected long patX, patY, patZ;

	protected AdjacencyList adjY, adjZ;
	protected long posY, posZ, minY, minZ, maxY, maxZ;
	protected long nextY, nextZ;
	protected long x, y, z;

	protected BitmapTriplesIterator(BitmapTriplesIndex idx, TripleID pattern, boolean search) {
		this.idx = idx;
		this.returnTriple = new TripleID();
		this.pattern = new TripleID();
		if (search) {
			newSearch(pattern);
		}
	}

	public BitmapTriplesIterator(BitmapTriplesIndex idx, TripleID pattern) {
		this(idx, pattern, true);
	}

	public BitmapTriplesIterator(BitmapTriplesIndex idx, long minZ, long maxZ) {
		this.idx = idx;
		this.returnTriple = new TripleID();
		this.pattern = new TripleID();
		adjY = idx.getAdjacencyListY();
		adjZ = idx.getAdjacencyListZ();

		this.minZ = minZ;
		this.maxZ = maxZ;
		this.minY = adjZ.findListIndex(minZ);
		this.maxY = adjZ.findListIndex(maxZ);
		goToStart();
	}

	public void newSearch(TripleID pattern) {
		this.pattern.assign(pattern);

		TripleOrderConvert.swapComponentOrder(this.pattern, TripleComponentOrder.SPO, idx.getOrder());
		patX = this.pattern.getSubject();
		patY = this.pattern.getPredicate();
		patZ = this.pattern.getObject();

		adjY = idx.getAdjacencyListY();
		adjZ = idx.getAdjacencyListZ();

		// ((BitSequence375)triples.bitmapZ).dump();

		findRange();
		goToStart();
	}

	protected void updateOutput() {
		lastPosition = posZ;
		returnTriple.setAll(x, y, z);
		TripleOrderConvert.swapComponentOrder(returnTriple, idx.getOrder(), TripleComponentOrder.SPO);
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
		return idx.getOrder();
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

	@Override
	public boolean isLastTriplePositionBoundToOrder() {
		return true;
	}

	private boolean gotoOrder(long id, TripleComponentRole role) {
		switch (role) {
			case SUBJECT -> {
				if (patX != 0) {
					return id == patX; // can't jump or already on the right element
				}

				patX = id;
				findRange();
				patX = 0;

				return true; // we know x exists because we are using
			}
			case PREDICATE -> {
				if (patY != 0) {
					return id == patY; // can't jump or already on the right element
				}

				if (posY == nextY) {
					return false; // no next element
				}

				long curr = this.adjY.get(posY);

				if (curr >= id) {
					return curr == id;
				}
				if (posY + 1 == nextY) {
					return false; // no next element
				}

				long last = this.adjY.get(nextY - 1);


				boolean res;

				if (last > id) {
					// binary search between curr <-> last id
					long loc = this.adjY.searchLoc(id, posY + 1, nextY - 2);

					if (loc > 0) {
						res = true;
						posY = loc;
						y = id;
					} else {
						res = false;
						posY = -loc - 1;
						y = adjY.get(posY);
					}
				} else if (last != id) {
					// last < id - GOTO end + 1
					posY = nextY;
					res = false;
				} else {
					// last == id - GOTO last
					posY = nextY - 1;
					y = adjY.get(posY);
					res = true;
				}

				nextY = adjY.findNext(posY) + 1;

				// down to z/posZ/nextZ?
				posZ = adjZ.find(posY, patZ);
				nextZ = adjZ.findNext(posZ) + 1;

				return res;
			}
			case OBJECT -> {
				if (patZ != 0) {
					return id == patZ; // can't jump or already on the right element
				}

				if (posZ == nextZ) {
					return false; // no next element
				}

				long curr = this.adjZ.get(posZ);

				if (curr >= id) {
					return curr == id;
				}
				if (posZ + 1 == nextZ) {
					return false; // no next element
				}

				long last = this.adjZ.get(nextZ - 1);


				boolean res;

				if (last > id) {
					// binary search between curr <-> last id
					long loc = this.adjZ.searchLoc(id, posZ + 1, nextZ - 2);

					if (loc >= 0) { //match
						res = true;
						posZ = loc;
						//z = id; // no need to compute the z, it is only used in next()
					} else {
						res = false;
						posZ = -loc - 1;
						//z = adjZ.get(posZ);
					}
				} else if (last != id) {
					// last < id - GOTO end
					posZ = nextZ;
					res = false;
				} else {
					// last == id - GOTO last
					posZ = nextZ - 1;
					//z = adjZ.get(posZ);
					res = true;
				}

				nextZ = adjZ.findNext(posZ) + 1;

				return res;
			}
			default -> throw new NotImplementedException("goto " + role);
		}
	}

	@Override
	public boolean gotoSubject(long id) {
		return gotoOrder(id, idx.getOrder().getSubjectMapping());
	}

	@Override
	public boolean gotoPredicate(long id) {
		return gotoOrder(id, idx.getOrder().getPredicateMapping());
	}
	@Override
	public boolean gotoObject(long id) {
		return gotoOrder(id, idx.getOrder().getObjectMapping());
	}

	@Override
	public boolean canGoToSubject() {
		return true;
	}
	@Override
	public boolean canGoToPredicate() {
		return true;
	}
	@Override
	public boolean canGoToObject() {
		return true;
	}

}

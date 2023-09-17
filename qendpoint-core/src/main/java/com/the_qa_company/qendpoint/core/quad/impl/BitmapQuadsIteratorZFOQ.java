package com.the_qa_company.qendpoint.core.quad.impl;

import java.util.List;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIteratorZFOQ;
import com.the_qa_company.qendpoint.core.triples.impl.TripleOrderConvert;

public class BitmapQuadsIteratorZFOQ extends BitmapTriplesIteratorZFOQ {

	// resolves ?PO?, ??O? queries

	private final List<? extends Bitmap> bitmapsGraph; // one bitmap per graph
	private final int numberOfGraphs;
	private long posG; // the current graph bitmap
	private int g; // g is variable
	private boolean updateXYZ = true;

	public BitmapQuadsIteratorZFOQ(BitmapTriples triples, TripleID pattern) {
		super(triples, pattern);
		this.bitmapsGraph = triples.getQuadInfoAG();
		this.numberOfGraphs = bitmapsGraph.size();
		newSearch(pattern);
	}

	@Override
	public void goToStart() {
		super.goToStart();
		if (hasNext()) {
			posG = getNextGraphPosition(0);
		}
	}

	/*
	 * Get the next solution
	 */
	@Override
	public TripleID next() {
		if (updateXYZ) {
			long posY = adjIndex.get(posIndex); // get the position of the next
												// occurrence of the predicate
												// in AdjY

			z = patZ != 0 ? patZ : (int) adjIndex.findListIndex(posIndex) + 1; // get
																				// the
																				// next
																				// object
																				// (z)
																				// as
																				// the
																				// number
																				// of
																				// list
																				// in
																				// adIndex
																				// corresponding
																				// to
																				// posIndex
			y = patY != 0 ? patY : (int) adjY.get(posY); // get the next
															// predicate (y) as
															// the element in
															// adjY stores in
															// position posY
			x = (int) adjY.findListIndex(posY) + 1; // get the next subject (X)
													// as the number of list in
													// adjY corresponding to
													// posY

			updateXYZ = false;
		}
		g = (int) posG + 1;

		posG = getNextGraphPosition((int) posG + 1); // get the next graph
														// position for the
														// current triple
		if (posG == numberOfGraphs) { // there are no further graphs for this
										// triple
			posIndex++; // increase the position of the next occurrence of the
						// predicate
			if (hasNext()) {
				updateXYZ = true;
				posG = getNextGraphPosition(0);
			}
		}

		updateOutput(); // set the components (subject,predicate,object) of the
						// returned triple
		return returnTriple; // return the triple as solution
	}

	private int getNextGraphPosition(int pos) {
		int nextTriplePos = (int) getNextTriplePosition();
		while (pos < numberOfGraphs && !bitmapsGraph.get(pos).access(nextTriplePos)) {
			pos++;
		}
		return pos;
	}

	private long getNextTriplePosition() {
		try {
			return triples.getAdjacencyListZ().find(adjIndex.get(posIndex), patZ);
		} catch (Exception ignore) {
			return 0;
		}
	}

	@Override
	protected void updateOutput() {
		returnTriple.setAll(x, y, z, g);
		TripleOrderConvert.swapComponentOrder(returnTriple, triples.getOrder(), TripleComponentOrder.SPO);
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.MORE_THAN;
	}

	private void newSearch(TripleID pattern) {
		this.pattern.assign(pattern);

		TripleOrderConvert.swapComponentOrder(this.pattern, TripleComponentOrder.SPO, triples.getOrder());
		patZ = this.pattern.getObject();
		if (patZ == 0 && (patY != 0 || this.pattern.getSubject() != 0)) {
			throw new IllegalArgumentException("This structure is not meant to process this pattern");
		}

		patY = this.pattern.getPredicate();

		adjY = triples.getAdjacencyListY();
		adjIndex = triples.getAdjacencyListIndex(); // adjIndex has the list of
													// positions in adY

		findRange(); // get the boundaries where the solution for the given
						// object can be found
		goToStart(); // load the first solution and position the next pointers
	}

	private void findRange() {
		if (patZ == 0) { // if the object is not provided (usually it is in this
							// iterator)
			minIndex = 0;
			maxIndex = adjIndex.getNumberOfElements();
			return;
		}
		minIndex = adjIndex.find(patZ - 1); // find the position of the first
											// occurrence of the object
		maxIndex = adjIndex.last(patZ - 1); // find the position of the last
											// ocurrence of the object

		if (patY != 0) { // if the predicate is provided then we do a binary
							// search to search for such predicate
			while (minIndex <= maxIndex) {
				long mid = (minIndex + maxIndex) / 2;
				long predicate = getY(mid); // get predicate at mid position in
											// the object index

				if (patY > predicate) {
					minIndex = mid + 1;
				} else if (patY < predicate) {
					maxIndex = mid - 1;
				} else { // the predicate has been found, now we have to find
							// the min and max limits (the predicate P is
							// repeated for each PO occurrence in the triples)
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

	@Override
	public boolean hasPrevious() {
		throw new NotImplementedException();
	}

	@Override
	public TripleID previous() {
		throw new NotImplementedException();
	}

	@Override
	public void goTo(long pos) {
		throw new NotImplementedException();
	}
}

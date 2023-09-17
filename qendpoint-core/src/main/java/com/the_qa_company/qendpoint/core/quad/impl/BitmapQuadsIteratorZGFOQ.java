package com.the_qa_company.qendpoint.core.quad.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIteratorZFOQ;
import com.the_qa_company.qendpoint.core.triples.impl.TripleOrderConvert;

public class BitmapQuadsIteratorZGFOQ extends BitmapTriplesIteratorZFOQ {
	// resolves ?POG, ??OG queries

	private final Bitmap bitmapGraph; // the bitmap of the requested graph

	public BitmapQuadsIteratorZGFOQ(BitmapTriples triples, TripleID pattern) {
		super(triples, pattern);
		this.bitmapGraph = triples.getQuadInfoAG().get((int) pattern.getGraph() - 1);
		newSearch(pattern);
	}

	protected void findRange() {
		findRange2();
		while (maxIndex >= minIndex && !bitmapGraph.access(getTriplePosition(maxIndex))) {
			maxIndex--;
		}

		while (maxIndex >= minIndex && !bitmapGraph.access(getTriplePosition(minIndex))) {
			minIndex++;
		}
	}

	/*
	 * Check if there are more solution
	 */
	@Override
	public boolean hasNext() {
		return posIndex <= maxIndex && maxIndex >= minIndex;
	}

	/*
	 * Get the next solution
	 */
	@Override
	public TripleID next() {
		long posY = adjIndex.get(posIndex); // get the position of the next
											// occurrence of the predicate in
											// AdjY

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
		y = patY != 0 ? patY : (int) adjY.get(posY); // get the next predicate
														// (y) as the element in
														// adjY stores in
														// position posY
		x = (int) adjY.findListIndex(posY) + 1; // get the next subject (X) as
												// the number of list in adjY
												// corresponding to posY

		do {
			posIndex++; // increase the position of the next occurrence of the
						// predicate
		} while (posIndex < maxIndex && !bitmapGraph.access(getNextTriplePosition()));

		updateOutput(); // set the components (subject,predicate,object) of the
						// returned triple
		return returnTriple; // return the triple as solution
	}

	public long getTriplePosition(long index) {
		try {
			return triples.getAdjacencyListZ().find(adjIndex.get(index), patZ);
		} catch (Exception ignore) {
			return 0;
		}
	}

	protected void newSearch(TripleID pattern) {
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

	protected void findRange2() {
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

	public long getNextTriplePosition() {
		try {
			return triples.getAdjacencyListZ().find(adjIndex.get(posIndex), patZ);
		} catch (Exception ignore) {
			return 0;
		}
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.UP_TO;
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

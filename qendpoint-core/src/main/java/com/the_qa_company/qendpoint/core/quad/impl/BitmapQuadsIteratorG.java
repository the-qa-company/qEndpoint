package com.the_qa_company.qendpoint.core.quad.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIterator;

public class BitmapQuadsIteratorG extends BitmapTriplesIterator {

	// resolves ???G, S??G, SP?G, SPOG queries

	private final Bitmap bitmapGraph; // the graph bitmap for the search

	public BitmapQuadsIteratorG(BitmapTriples triples, TripleID pattern) {
		super(triples, pattern, false);
		this.bitmapGraph = triples.getQuadInfoAG().get((int) pattern.getGraph() - 1);
		newSearch(pattern);
	}

	@Override
	public void goToStart() {
		if (minZ >= maxZ || minZ == -1) { // no results
			posZ = maxZ;
		} else {
			super.goToStart();
		}
		if (!bitmapGraph.access(posZ)) {
			posZ = bitmapGraph.selectNext1(posZ + 1);
		}
	}

	@Override
	public long estimatedNumResults() {
		if (minZ == -1) {
			return 0;
		}
		return bitmapGraph.rank1(maxZ - 1) - bitmapGraph.rank1(minZ - 1);
	}

	/*
	 * Check if there are more solution
	 */
	@Override
	public boolean hasNext() {
		return posZ < maxZ && posZ != -1; // Just check if we have arrived to
											// the maximum position of the
											// objects that resolve the query
	}

	/*
	 * Get the next solution
	 */
	@Override
	public TripleID next() {
		z = adjZ.get(posZ); // get the next object (Z). We just retrieve it from
							// the list of objects (AdjZ) from current position
							// posZ
		if (posZ >= nextZ) { // if, with the current position of the object
								// (posZ), we have reached the next list of
								// objects (starting in nexZ), then we should
								// update the associated predicate (Y) and,
								// potentially, also the associated subject (X)
			posY = triples.getBitmapZ().rank1(posZ - 1); // move to the next
															// position of
															// predicates
			y = adjY.get(posY); // get the next predicate (Y). We just retrieve
								// it from the list of predicates(AdjY) from
								// current position posY
			nextZ = adjZ.findNext(posZ) + 1; // update nextZ, storing in which
												// position (in adjZ) ends the
												// list of objects associated
												// with the current
												// subject,predicate
			if (posY >= nextY) { // if we have reached the next list of objects
									// (starting in nexZ) we should update the
									// associated predicate (Y) and,
									// potentially, also the associated subject
									// (X)
				x = triples.getBitmapY().rank1(posY - 1) + 1; // get the next
																// subject (X)
				nextY = adjY.findNext(posY) + 1; // update nextY, storing in
													// which position (in AdjY)
													// ends the list of
													// predicates associated
													// with the current subject
			}
		}
		posZ = bitmapGraph.selectNext1(posZ + 1);

		updateOutput(); // set the components (subject,predicate,object,graph)
						// of the returned triple
		return returnTriple; // return the triple as solution
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
	public boolean canGoTo() {
		throw new NotImplementedException();
	}

	@Override
	public void goTo(long pos) {
		throw new NotImplementedException();
	}

}

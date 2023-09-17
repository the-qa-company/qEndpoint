package com.the_qa_company.qendpoint.core.quad.impl;

import java.util.List;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIterator;
import com.the_qa_company.qendpoint.core.triples.impl.TripleOrderConvert;

public class BitmapQuadsIterator extends BitmapTriplesIterator {

	// resolves ????, S???, SP??, SPO? queries

	private final List<? extends Bitmap> bitmapsGraph; // one bitmap per graph
	private final long numberOfGraphs;
	private long posG; // the current graph bitmap
	private long g; // g is variable

	public BitmapQuadsIterator(BitmapTriples triples, TripleID pattern) {
		super(triples, pattern, false);
		this.bitmapsGraph = triples.getQuadInfoAG();
		this.numberOfGraphs = bitmapsGraph.size();
		newSearch(pattern);
	}

	@Override
	public void goToStart() {
		super.goToStart();
		posG = 0;
		while (!bitmapsGraph.get((int) posG).access(posZ)) {
			posG++;
		}
		g = posG + 1;
	}

	@Override
	public long estimatedNumResults() {
		long results = 0;
		for (int i = 0; i < numberOfGraphs; i++) {
			results += bitmapsGraph.get(i).rank1(maxZ - 1) - bitmapsGraph.get(i).rank1(minZ - 1);
		}
		return results;
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

		g = posG + 1;

		// set posG to the next graph of this triple
		do {
			posG++;
		} while (posG + 1 <= numberOfGraphs && !bitmapsGraph.get((int) posG).access(posZ));

		if (posG == numberOfGraphs) { // there are no further graphs for this
										// triple
			posZ++;
			if (posZ < maxZ) {
				posG = 0;
				while (!bitmapsGraph.get((int) posG).access(posZ)) {
					posG++;
				}
			}
		}

		updateOutput(); // set the components (subject,predicate,object,graph)
						// of the returned triple
		return returnTriple; // return the triple as solution
	}

	/*
	 * Set the components (subject,predicate,object) of the returned triple
	 */
	@Override
	protected void updateOutput() {
		returnTriple.setAll(x, y, z, g);
		TripleOrderConvert.swapComponentOrder(returnTriple, triples.getOrder(), TripleComponentOrder.SPO);
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

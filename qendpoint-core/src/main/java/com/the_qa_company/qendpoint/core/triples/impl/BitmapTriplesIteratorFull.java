package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.NoSuchElementException;

public class BitmapTriplesIteratorFull implements SuppliableIteratorTripleID {

	protected final BitmapTriplesIndex idx;
	protected final Bitmap bitY, bitZ;
	protected final Sequence seqY, seqZ;
	protected final TripleComponentOrder order;
	protected final TripleID returnTriple = new TripleID();
	protected long maxZ;
	protected long posY, posZ;

	public BitmapTriplesIteratorFull(BitmapTriplesIndex idx, long maxZ, TripleComponentOrder order) {
		this.idx = idx;
		this.order = order;
		bitY = idx.getBitmapY();
		bitZ = idx.getBitmapZ();
		seqY = idx.getSeqY();
		seqZ = idx.getSeqZ();

		this.maxZ = maxZ;

		goToStart();
	}

	@Override
	public boolean hasPrevious() {
		return false;
	}

	@Override
	public TripleID previous() {
		throw new NoSuchElementException();
	}

	@Override
	public void goToStart() {
		posY = 0;
		posZ = 0;

		if (maxZ == 0)
			return; // nothing

		returnTriple.setAll(1, seqY.get(0), 0);
	}

	@Override
	public boolean canGoTo() {
		return false;
	}

	@Override
	public void goTo(long pos) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long estimatedNumResults() {
		return maxZ;
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.EXACT;
	}

	@Override
	public TripleComponentOrder getOrder() {
		return order;
	}

	@Override
	public long getLastTriplePosition() {
		return posZ;
	}

	@Override
	public boolean hasNext() {
		return posZ < maxZ;
	}

	@Override
	public TripleID next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		returnTriple.setObject(seqZ.get(posZ));

		if (posZ != 0 && bitZ.access(posZ - 1)) {
			posY++;
			returnTriple.setPredicate(seqY.get(posY));

			if (posY != 0 && bitY.access(posY - 1)) {
				returnTriple.setSubject(returnTriple.getSubject() + 1);
			}

		}

		posZ++;

		return returnTriple;
	}
}

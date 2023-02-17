package com.the_qa_company.qendpoint.core.hdt.impl.diskindex;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.util.io.compress.Pair;

public class ObjectAdjReader extends FetcherIterator<Pair> {
	private final Sequence seqZ, seqY;
	private final Bitmap bitmapZ;
	private long indexY, indexZ;

	public ObjectAdjReader(Sequence seqZ, Sequence seqY, Bitmap bitmapZ) {
		this.seqZ = seqZ;
		this.seqY = seqY;
		this.bitmapZ = bitmapZ;
	}

	@Override
	protected Pair getNext() {
		if (indexZ >= seqZ.getNumberOfElements()) {
			return null;
		}

		Pair pair = new Pair();
		// create a pair object
		pair.object = seqZ.get(indexZ);
		pair.predicatePosition = indexY;
		pair.predicate = seqY.get(indexY);

		// shift to the next predicate if required
		if (bitmapZ.access(indexZ++)) {
			indexY++;
		}
		return pair;
	}
}

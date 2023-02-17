package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryDiff;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatMapping;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.SectionUtil;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleIDComparator;
import com.the_qa_company.qendpoint.core.triples.Triples;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BitmapTriplesIteratorMapDiff implements IteratorTripleID {
	private final CatMapping subjMapping;
	private final CatMapping objMapping;
	private final CatMapping predMapping;
	private final CatMapping sharedMapping;

	private final long countTriples;

	DictionaryDiff dictionaryDiff;

	Iterator<TripleID> list;
	Triples triples;
	TripleIDComparator tripleIDComparator = new TripleIDComparator(TripleComponentOrder.SPO);
	Bitmap bitArrayDisk;

	public BitmapTriplesIteratorMapDiff(HDT hdtOriginal, Bitmap deleteBitmap, DictionaryDiff dictionaryDiff) {
		this.subjMapping = dictionaryDiff.getAllMappings().get(SectionUtil.SECTION_SUBJECT);
		this.objMapping = dictionaryDiff.getAllMappings().get(SectionUtil.SECTION_OBJECT);
		this.predMapping = dictionaryDiff.getAllMappings().get(SectionUtil.SECTION_PREDICATE);
		this.sharedMapping = dictionaryDiff.getAllMappings().get(SectionUtil.SECTION_SHARED);
		this.dictionaryDiff = dictionaryDiff;
		this.countTriples = Math.max(0, hdtOriginal.getTriples().getNumberOfElements() - deleteBitmap.countOnes());
		this.triples = hdtOriginal.getTriples();
		this.bitArrayDisk = deleteBitmap;
		list = getTripleID(0).listIterator();
		count++;
	}

	@Override
	public boolean hasPrevious() {
		return false;
	}

	@Override
	public TripleID previous() {
		return null;
	}

	@Override
	public void goToStart() {

	}

	@Override
	public boolean canGoTo() {
		return false;
	}

	@Override
	public void goTo(long pos) {

	}

	@Override
	public long estimatedNumResults() {
		return countTriples;
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return null;
	}

	@Override
	public TripleComponentOrder getOrder() {
		return null;
	}

	@Override
	public long getLastTriplePosition() {
		throw new NotImplementedException();
	}

	private long count;

	@Override
	public boolean hasNext() {
		return count < dictionaryDiff.getMappingBack().getSize() || list.hasNext();
	}

	@Override
	public TripleID next() {
		if (!list.hasNext()) {
			list = getTripleID(count).iterator();
			count++;
		}
		return list.next();
	}

	private List<TripleID> getTripleID(long count) {
		ArrayList<TripleID> newTriples = new ArrayList<>();
		if (dictionaryDiff.getMappingBack().getSize() > 0) {
			long mapping = dictionaryDiff.getMappingBack().getMapping(count);
			IteratorTripleID it = this.triples.search(new TripleID(mapping, 0, 0));
			while (it.hasNext()) {
				TripleID next = it.next();
				if (!this.bitArrayDisk.access(it.getLastTriplePosition())) {
					newTriples.add(mapTriple(next));
				}
			}
		}
		newTriples.sort(tripleIDComparator);
		return newTriples;
	}

	public TripleID mapTriple(TripleID tripleID) {

		long subjOld = tripleID.getSubject();
		long numShared = this.sharedMapping.getSize();
		long newSubjId;
		if (subjOld <= numShared) {
			if (this.sharedMapping.getType(subjOld - 1) == 1) {
				newSubjId = this.sharedMapping.getMapping(subjOld - 1);
			} else {
				newSubjId = this.sharedMapping.getMapping(subjOld - 1) + this.dictionaryDiff.getNumShared();
			}
		} else {
			if (this.subjMapping.getType(subjOld - numShared - 1) == 1)
				newSubjId = this.subjMapping.getMapping(subjOld - numShared - 1);
			else
				newSubjId = this.subjMapping.getMapping(subjOld - numShared - 1) + this.dictionaryDiff.getNumShared();
		}
		long newPredId = this.predMapping.getMapping(tripleID.getPredicate() - 1);

		long objOld = tripleID.getObject();
		long newObjId;
		if (objOld <= numShared) {
			long type = this.sharedMapping.getType(objOld - 1);
			if (type == 1) {
				newObjId = this.sharedMapping.getMapping(objOld - 1);
			} else {
				newObjId = this.sharedMapping.getMapping(objOld - 1) + this.dictionaryDiff.getNumShared();
			}

		} else {
			if (this.objMapping.getType(objOld - numShared - 1) == 1)
				newObjId = this.objMapping.getMapping(objOld - numShared - 1);
			else
				newObjId = this.objMapping.getMapping(objOld - numShared - 1) + this.dictionaryDiff.getNumShared();
		}
		return new TripleID(newSubjId, newPredId, newObjId);
	}
}

package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.AdjacencyList;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;

public interface BitmapTriplesIndex {
	Bitmap getBitmapY();

	Bitmap getBitmapZ();

	Sequence getSeqY();

	Sequence getSeqZ();

	AdjacencyList getAdjacencyListY();

	AdjacencyList getAdjacencyListZ();

	TripleComponentOrder getOrder();
}

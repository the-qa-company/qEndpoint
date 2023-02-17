package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MergeExceptionIterator;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleIDComparator;

import java.io.IOException;

/**
 * Version of {@link MergeExceptionIterator} with {@link TripleID}
 *
 * @author Antoine Willerval
 */
public class CompressTripleMergeIterator extends MergeExceptionIterator<TripleID, IOException> {

	public CompressTripleMergeIterator(ExceptionIterator<TripleID, IOException> in1,
			ExceptionIterator<TripleID, IOException> in2, TripleComponentOrder order) {
		super(in1, in2, TripleIDComparator.getComparator(order));
	}

	public static <T extends ExceptionIterator<TripleID, IOException>> ExceptionIterator<TripleID, IOException> buildOfTree(
			T[] lst, TripleComponentOrder order) {
		return buildOfTree(it -> it, TripleIDComparator.getComparator(order), lst, 0, lst.length);
	}
}

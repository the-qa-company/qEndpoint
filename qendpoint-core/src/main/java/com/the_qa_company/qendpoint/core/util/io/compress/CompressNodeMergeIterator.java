package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MergeExceptionIterator;

import java.io.IOException;
import java.util.Comparator;

/**
 * Version of {@link MergeExceptionIterator} with {@link IndexedNode}
 *
 * @author Antoine Willerval
 */
public class CompressNodeMergeIterator extends MergeExceptionIterator<IndexedNode, IOException> {

	public CompressNodeMergeIterator(ExceptionIterator<IndexedNode, IOException> in1,
			ExceptionIterator<IndexedNode, IOException> in2) {
		super(in1, in2, IndexedNode::compareTo);
	}

	public static <T extends ExceptionIterator<IndexedNode, IOException>> ExceptionIterator<IndexedNode, IOException> buildOfTree(
			T[] lst) {
		return buildOfTree(lst, IndexedNode::compareTo);
	}

	public static <T extends ExceptionIterator<IndexedNode, IOException>> ExceptionIterator<IndexedNode, IOException> buildOfTree(
			T[] lst, Comparator<IndexedNode> comparator) {
		return buildOfTree(it -> it, comparator, lst, 0, lst.length);
	}
}

package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.RawStringUtils;

import java.util.Comparator;

public class RawSectionCompressor extends SectionCompressor implements Comparator<IndexedNode> {
	public RawSectionCompressor(CloseSuppressPath baseFileName, AsyncIteratorFetcher<TripleString> source,
			MultiThreadListener listener, int bufferSize, long chunkSize, int k, boolean debugSleepKwayDict,
			boolean quads) {
		super(baseFileName, source, listener, bufferSize, chunkSize, k, debugSleepKwayDict, quads);
		setComparator(this);
	}

	@Override
	public int compare(IndexedNode o1, IndexedNode o2) {
		return RawStringUtils.compareRawString(o1.getNode(), o2.getNode());
	}

	@Override
	protected ByteString convertObject(CharSequence seq) {
		return RawStringUtils.convertToRawString(seq);
	}
}

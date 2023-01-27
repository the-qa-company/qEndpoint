package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

/**
 * Implementation of SectionCompressor for MultiSection
 */
public class MultiSectionSectionCompressor extends SectionCompressor {
	public MultiSectionSectionCompressor(CloseSuppressPath baseFileName, AsyncIteratorFetcher<TripleString> source,
			MultiThreadListener listener, int bufferSize, long chunkSize, int k, boolean debugSleepKwayDict) {
		super(baseFileName, source, listener, bufferSize, chunkSize, k, debugSleepKwayDict);
	}

	@Override
	protected ByteString convertObject(CharSequence seq) {
		return new CompactString(LiteralsUtils.litToPref(seq));
	}
}

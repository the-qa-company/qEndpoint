package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

public class MultiSectionLangSectionCompressor extends SectionCompressor {
	public MultiSectionLangSectionCompressor(CloseSuppressPath baseFileName, AsyncIteratorFetcher<TripleString> source,
			MultiThreadListener listener, int bufferSize, long chunkSize, int k, boolean debugSleepKwayDict, boolean quad) {
		super(baseFileName, source, listener, bufferSize, chunkSize, k, debugSleepKwayDict, quad);
	}

	@Override
	protected ByteString convertObject(CharSequence seq) {
		// no need to create a new ByteString from it knowing it's already a new
		// one
		return LiteralsUtils.litToPrefLang(seq);
	}
}

package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.PrefixesStorage;

public class MultiSectionLangPrefixSectionCompressor extends SectionCompressor {
	private final PrefixesStorage prefixes;

	public MultiSectionLangPrefixSectionCompressor(CloseSuppressPath baseFileName,
			AsyncIteratorFetcher<TripleString> source, MultiThreadListener listener, int bufferSize, long chunkSize,
			int k, boolean debugSleepKwayDict, boolean quad, HDTOptions spec, CompressionType compressionType) {
		super(baseFileName, source, listener, bufferSize, chunkSize, k, debugSleepKwayDict, quad, compressionType);
		this.prefixes = new PrefixesStorage();
		this.prefixes.loadConfig(spec.get(HDTOptionsKeys.LOADER_PREFIXES));
	}

	@Override
	protected ByteString convertObject(CharSequence seq) {
		return LiteralsUtils.litToPrefLangCut(seq, prefixes);
	}

	@Override
	protected ByteString convertSubject(CharSequence seq) {
		return LiteralsUtils.resToPrefLangCut(seq, prefixes);
	}

	@Override
	protected ByteString convertPredicate(CharSequence seq) {
		return LiteralsUtils.resToPrefLangCut(seq, prefixes);
	}

}

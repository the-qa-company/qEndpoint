package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.impl.OneReadTempTriples;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressTripleReader;

import java.io.IOException;

/**
 * Implementation of {@link TripleCompressionResult} for full file reading
 *
 * @author Antoine Willerval
 */
public class TripleCompressionResultFile implements TripleCompressionResult {
	private final long tripleCount;
	private final CompressTripleReader reader;
	private final TripleComponentOrder order;
	private final CloseSuppressPath triples;

	public TripleCompressionResultFile(long tripleCount, CloseSuppressPath triples, TripleComponentOrder order,
			int bufferSize) throws IOException {
		this.tripleCount = tripleCount;
		this.reader = new CompressTripleReader(triples.openInputStream(bufferSize));
		this.order = order;
		this.triples = triples;
	}

	@Override
	public TempTriples getTriples() {
		return new OneReadTempTriples(reader.asIterator(), order, tripleCount);
	}

	@Override
	public long getTripleCount() {
		return tripleCount;
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(reader, triples);
	}
}

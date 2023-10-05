package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressTripleReader;
import com.the_qa_company.qendpoint.core.triples.impl.OneReadTempTriples;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

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
	private final long graphs;

	public TripleCompressionResultFile(long tripleCount, CloseSuppressPath triples, TripleComponentOrder order,
									   int bufferSize, long graphs) throws IOException {
		this.tripleCount = tripleCount;
		this.graphs = graphs;
		this.reader = new CompressTripleReader(triples.openInputStream(bufferSize));
		this.order = order;
		this.triples = triples;
	}

	@Override
	public TempTriples getTriples() {
		return new OneReadTempTriples(reader.asIterator(), order, tripleCount, graphs);
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

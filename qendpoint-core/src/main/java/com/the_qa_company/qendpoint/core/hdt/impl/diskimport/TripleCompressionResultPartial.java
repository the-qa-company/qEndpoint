package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressTripleMergeIterator;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressTripleReader;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.triples.impl.OneReadTempTriples;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link TripleCompressionResult} for partial file reading
 *
 * @author Antoine Willerval
 */
public class TripleCompressionResultPartial implements TripleCompressionResult {
	private final List<CompressTripleReader> files;
	private final TempTriples triples;
	private final long tripleCount;
	private final TripleComponentOrder order;

	public TripleCompressionResultPartial(List<CloseSuppressPath> files, long tripleCount, TripleComponentOrder order,
			int bufferSize, long graphs, long shared) throws IOException {
		this.files = new ArrayList<>(files.size());
		this.tripleCount = tripleCount;
		this.order = order;
		this.triples = new OneReadTempTriples(createBTree(files, 0, files.size(), bufferSize).asIterator(), order,
				tripleCount, graphs, shared);
	}

	private ExceptionIterator<TripleID, IOException> createBTree(List<CloseSuppressPath> files, int start, int end,
			int bufferSize) throws IOException {
		int size = end - start;
		if (size <= 0) {
			return ExceptionIterator.empty();
		}
		if (size == 1) {
			CompressTripleReader r = new CompressTripleReader(files.get(start).openInputStream(bufferSize));
			this.files.add(r);
			return r;
		}
		int mid = (start + end) / 2;
		ExceptionIterator<TripleID, IOException> left = createBTree(files, start, mid, bufferSize);
		ExceptionIterator<TripleID, IOException> right = createBTree(files, mid, end, bufferSize);
		return new CompressTripleMergeIterator(left, right, order);
	}

	@Override
	public TempTriples getTriples() {
		return triples;
	}

	@Override
	public long getTripleCount() {
		return tripleCount;
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(files);
	}
}

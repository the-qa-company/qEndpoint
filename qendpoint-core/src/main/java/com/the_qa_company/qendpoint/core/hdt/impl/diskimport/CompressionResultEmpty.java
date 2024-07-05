package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;

import java.io.IOException;

public class CompressionResultEmpty implements CompressionResult {
	@Override
	public long getTripleCount() {
		return 0;
	}

	@Override
	public boolean supportsGraph() {
		return false;
	}

	@Override
	public boolean hasOnlyBaseLiterals() {
		return true;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getSubjects() {
		return ExceptionIterator.empty();
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getPredicates() {
		return ExceptionIterator.empty();
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getObjects() {
		return ExceptionIterator.empty();
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getGraph() {
		return ExceptionIterator.empty();
	}

	@Override
	public long getSubjectsCount() {
		return 0;
	}

	@Override
	public long getPredicatesCount() {
		return 0;
	}

	@Override
	public long getObjectsCount() {
		return 0;
	}

	@Override
	public long getGraphCount() {
		return 0;
	}

	@Override
	public long getSharedCount() {
		return 0;
	}

	@Override
	public void delete() throws IOException {
	}

	@Override
	public long getRawSize() {
		return 0;
	}

	@Override
	public void close() throws IOException {
	}
}

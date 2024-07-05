package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressNodeReader;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;

/**
 * Implementation of {@link CompressionResult} for full file reading
 *
 * @author Antoine Willerval
 */
public class CompressionResultFile implements CompressionResult {
	private final long tripleCount;
	private final long ntRawSize;
	private final CompressNodeReader subjects;
	private final CompressNodeReader predicates;
	private final CompressNodeReader objects;
	private final CompressNodeReader graph;
	private final SectionCompressor.TripleFile sections;
	private final boolean supportsGraph;
	private final boolean stringLiterals;

	public CompressionResultFile(long tripleCount, long ntRawSize, SectionCompressor.TripleFile sections,
			boolean supportsGraph, boolean stringLiterals) throws IOException {
		this.tripleCount = tripleCount;
		this.ntRawSize = ntRawSize;
		this.subjects = new CompressNodeReader(sections.openRSubject(), true);
		this.predicates = new CompressNodeReader(sections.openRPredicate(), true);
		this.objects = new CompressNodeReader(sections.openRObject(), stringLiterals);
		this.supportsGraph = supportsGraph;
		this.stringLiterals = stringLiterals;
		if (supportsGraph) {
			this.graph = new CompressNodeReader(sections.openRGraph(), true);
		} else {
			this.graph = null;
		}
		this.sections = sections;
	}

	@Override
	public long getTripleCount() {
		return tripleCount;
	}

	@Override
	public boolean supportsGraph() {
		return supportsGraph;
	}

	@Override
	public boolean hasOnlyBaseLiterals() {
		return stringLiterals;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getSubjects() {
		return subjects;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getPredicates() {
		return predicates;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getObjects() {
		return objects;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getGraph() {
		return graph;
	}

	@Override
	public void delete() throws IOException {
		sections.delete();
	}

	@Override
	public long getSubjectsCount() {
		return subjects.getSize();
	}

	@Override
	public long getPredicatesCount() {
		return predicates.getSize();
	}

	@Override
	public long getObjectsCount() {
		return objects.getSize();
	}

	@Override
	public long getGraphCount() {
		return graph.getSize();
	}

	@Override
	public long getSharedCount() {
		return tripleCount;
	}

	@Override
	public long getRawSize() {
		return ntRawSize;
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(objects, predicates, subjects, graph);
	}
}

package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressNodeReader;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.ICompressNodeReader;

import java.io.IOException;

/**
 * Implementation of {@link CompressionResult} for full file reading
 *
 * @author Antoine Willerval
 */
public class CompressionResultFile implements CompressionResult {
	private final long tripleCount;
	private final long ntRawSize;
	private final ICompressNodeReader subjects;
	private final ICompressNodeReader predicates;
	private final ICompressNodeReader objects;
	private final ICompressNodeReader graph;
	private final SectionCompressor.TripleFile sections;
	private final boolean supportsGraph;

	public CompressionResultFile(long tripleCount, long ntRawSize, SectionCompressor.TripleFile sections,
			boolean supportsGraph, boolean useRaw) throws IOException {
		this.tripleCount = tripleCount;
		this.ntRawSize = ntRawSize;
		this.subjects = ICompressNodeReader.of(sections.openRSubject(), useRaw);
		this.predicates = ICompressNodeReader.of(sections.openRPredicate(), useRaw);
		this.objects = ICompressNodeReader.of(sections.openRObject(), useRaw);
		this.supportsGraph = supportsGraph;
		if (supportsGraph) {
			this.graph = ICompressNodeReader.of(sections.openRGraph(), useRaw);
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

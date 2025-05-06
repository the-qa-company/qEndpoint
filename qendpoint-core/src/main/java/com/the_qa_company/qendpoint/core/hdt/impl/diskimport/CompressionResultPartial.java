package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressNodeMergeIterator;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressNodeReader;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.ICompressNodeReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Implementation of {@link CompressionResult} for partial file reading
 *
 * @author Antoine Willerval
 */
public class CompressionResultPartial implements CompressionResult {
	private final List<CompressNodeReaderTriple> files;
	private final long triplesCount;
	private final long ntSize;
	private final ExceptionIterator<IndexedNode, IOException> subject;
	private final ExceptionIterator<IndexedNode, IOException> predicate;
	private final ExceptionIterator<IndexedNode, IOException> object;
	private final ExceptionIterator<IndexedNode, IOException> graph;

	public CompressionResultPartial(List<SectionCompressor.TripleFile> files, long triplesCount, long ntSize,
			boolean graph, boolean useRaw) throws IOException {
		this.files = new ArrayList<>(files.size());
		this.ntSize = ntSize;
		for (SectionCompressor.TripleFile file : files) {
			this.files.add(new CompressNodeReaderTriple(file, graph, useRaw));
		}
		this.triplesCount = triplesCount;

		// building iterator trees
		this.subject = createBTree(0, files.size(), CompressNodeReaderTriple::getS);
		this.predicate = createBTree(0, files.size(), CompressNodeReaderTriple::getP);
		this.object = createBTree(0, files.size(), CompressNodeReaderTriple::getO);
		if (graph) {
			this.graph = createBTree(0, files.size(), CompressNodeReaderTriple::getG);
		} else {
			this.graph = null;
		}
	}

	private ExceptionIterator<IndexedNode, IOException> createBTree(int start, int end,
			Function<CompressNodeReaderTriple, ICompressNodeReader> fetcher) {
		int size = end - start;
		if (size <= 0) {
			return ExceptionIterator.empty();
		}
		if (size == 1) {
			return fetcher.apply(files.get(start));
		}
		int mid = (start + end) / 2;
		ExceptionIterator<IndexedNode, IOException> left = createBTree(start, mid, fetcher);
		ExceptionIterator<IndexedNode, IOException> right = createBTree(mid, end, fetcher);
		return new CompressNodeMergeIterator(left, right);
	}

	@Override
	public long getTripleCount() {
		return triplesCount;
	}

	@Override
	public boolean supportsGraph() {
		return graph != null;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getSubjects() {
		return subject;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getPredicates() {
		return predicate;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getObjects() {
		return object;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getGraph() {
		return graph;
	}

	@Override
	public void delete() throws IOException {
		IOUtil.closeAll(files);
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(files);
	}

	/*
	 * use the count of triples because we don't know the number of subjects
	 */
	@Override
	public long getSubjectsCount() {
		return triplesCount;
	}

	@Override
	public long getPredicatesCount() {
		return triplesCount;
	}

	@Override
	public long getObjectsCount() {
		return triplesCount;
	}

	@Override
	public long getSharedCount() {
		return triplesCount;
	}

	@Override
	public long getGraphCount() {
		return triplesCount;
	}

	@Override
	public long getRawSize() {
		return ntSize;
	}

	private static class CompressNodeReaderTriple implements Closeable {
		final ICompressNodeReader s, p, o, g;
		final SectionCompressor.TripleFile file;

		public CompressNodeReaderTriple(SectionCompressor.TripleFile file, boolean graph, boolean useRaw) throws IOException {
			this.s = ICompressNodeReader.of(file.openRSubject(), useRaw);
			this.p = ICompressNodeReader.of(file.openRPredicate(), useRaw);
			this.o = ICompressNodeReader.of(file.openRObject(), useRaw);
			if (graph) {
				this.g = ICompressNodeReader.of(file.openRGraph(), useRaw);
			} else {
				this.g = null;
			}
			this.file = file;
		}

		@Override
		public void close() throws IOException {
			IOUtil.closeAll(s, p, o, g);
		}

		public ICompressNodeReader getS() {
			return s;
		}

		public ICompressNodeReader getP() {
			return p;
		}

		public ICompressNodeReader getO() {
			return o;
		}

		public ICompressNodeReader getG() {
			return g;
		}
	}
}

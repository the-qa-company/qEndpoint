package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.DictionaryIDMapping;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.Triples;
import com.the_qa_company.qendpoint.core.util.io.compress.NoDuplicateTripleIDIterator;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * {@link TempTriples} only readable once with the {@link #searchAll()} method
 * with a predefined order, trying to set another order will lead to an
 * exception, trying to use any other method can lead to a
 * {@link NotImplementedException}.
 *
 * @author Antoine Willerval
 */
public class OneReadTempTriples implements TempTriples {
	private IteratorTripleID iterator;
	private TripleComponentOrder order;
	private long graphs;

	public OneReadTempTriples(Iterator<TripleID> iterator, TripleComponentOrder order, long triples) {
		this(iterator, order, triples, 0);
	}

	public OneReadTempTriples(Iterator<TripleID> iterator, TripleComponentOrder order, long triples, long graphs) {
		this.iterator = new SimpleIteratorTripleID(iterator, order, triples);
		this.order = order;
		this.graphs = graphs;
	}

	@Override
	public boolean insert(long subject, long predicate, long object) {
		throw new NotImplementedException();
	}

	@Override
	public boolean insert(long subject, long predicate, long object, long graph) {
		throw new NotImplementedException();
	}

	@Override
	public boolean insert(TripleID... triples) {
		throw new NotImplementedException();
	}

	@Override
	public boolean remove(TripleID... pattern) {
		throw new NotImplementedException();
	}

	@Override
	public void sort(ProgressListener listener) {
		// already sorted
	}

	@Override
	public void removeDuplicates(ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void setOrder(TripleComponentOrder order) {
		if (order != this.order) {
			throw new IllegalArgumentException("order asked by isn't the same as the set one!");
		}
	}

	@Override
	public void clear() {
		throw new NotImplementedException();
	}

	@Override
	public void load(Triples triples, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void replaceAllIds(DictionaryIDMapping mapSubj, DictionaryIDMapping mapPred, DictionaryIDMapping mapObj) {
		throw new NotImplementedException();
	}

	@Override
	public void replaceAllIds(DictionaryIDMapping mapSubj, DictionaryIDMapping mapPred, DictionaryIDMapping mapObj,
			DictionaryIDMapping mapGraph) {

	}

	@Override
	public void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public SuppliableIteratorTripleID search(TripleID pattern) {
		throw new NotImplementedException();
	}

	@Override
	public SuppliableIteratorTripleID search(TripleID pattern, int searchMask) {
		throw new NotImplementedException();
	}

	@Override
	public void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void mapFromFile(CountInputStream in, File f, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void generateIndex(ProgressListener listener, HDTOptions disk, Dictionary dictionary) {
		throw new NotImplementedException();
	}

	@Override
	public void loadIndex(InputStream input, ControlInfo ci, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void mapIndex(CountInputStream input, File f, ControlInfo ci, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void mapGenOtherIndexes(Path file, HDTOptions spec, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void saveIndex(OutputStream output, ControlInfo ci, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void load(TempTriples input, ProgressListener listener) {
		if (input instanceof OneReadTempTriples input2) {
			this.iterator = input2.iterator;
			this.order = input2.order;
			this.graphs = input2.graphs;
		} else {
			throw new NotImplementedException();
		}
	}

	@Override
	public TripleComponentOrder getOrder() {
		return order;
	}

	@Override
	public IteratorTripleID searchAll() {
		return new NoDuplicateTripleIDIterator(iterator);
	}

	@Override
	public IteratorTripleID searchAll(int searchMask) {
		return new NoDuplicateTripleIDIterator(iterator);
	}

	@Override
	public long getNumberOfElements() {
		return iterator.estimatedNumResults();
	}

	@Override
	public long size() {
		return iterator.estimatedNumResults();
	}

	@Override
	public void populateHeader(Header head, String rootNode) {
		throw new NotImplementedException();
	}

	@Override
	public String getType() {
		throw new NotImplementedException();
	}

	@Override
	public TripleID findTriple(long position, TripleID buffer) {
		throw new NotImplementedException();
	}

	@Override
	public List<TripleComponentOrder> getTripleComponentOrder(TripleID t) {
		return List.of();
	}

	@Override
	public void close() throws IOException {
		// nothing to do
	}

	@Override
	public long getGraphsCount() {
		return graphs;
	}

	private static class SimpleIteratorTripleID implements IteratorTripleID {
		private final Iterator<TripleID> it;
		private final TripleComponentOrder order;
		private final long tripleCount;

		public SimpleIteratorTripleID(Iterator<TripleID> it, TripleComponentOrder order, long tripleCount) {
			this.it = it;
			this.order = order;
			this.tripleCount = tripleCount;
		}

		@Override
		public void goToStart() {
			throw new NotImplementedException();
		}

		@Override
		public boolean canGoTo() {
			throw new NotImplementedException();
		}

		@Override
		public void goTo(long pos) {
			throw new NotImplementedException();
		}

		@Override
		public long estimatedNumResults() {
			return tripleCount;
		}

		@Override
		public ResultEstimationType numResultEstimation() {
			return ResultEstimationType.UP_TO;
		}

		@Override
		public TripleComponentOrder getOrder() {
			return order;
		}

		@Override
		public long getLastTriplePosition() {
			return tripleCount;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public TripleID next() {
			return it.next();
		}
	}
}

package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.SizeFetcher;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.ParallelSortableArrayList;
import com.the_qa_company.qendpoint.core.util.concurrent.KWayMerger;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressTripleMergeIterator;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressTripleReader;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressTripleWriter;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class DiskTriplesReorderSorter implements KWayMerger.KWayMergerImpl<TripleID, SizeFetcher<TripleID>> {
	private final CloseSuppressPath baseFileName;
	private final AsyncIteratorFetcher<TripleID> source;
	private final MultiThreadListener listener;
	private final int bufferSize;
	private final long chunkSize;
	private final int k;
	private final TripleComponentOrder oldOrder;
	private final TripleComponentOrder newOrder;
	private final AtomicLong read = new AtomicLong();

	public DiskTriplesReorderSorter(CloseSuppressPath baseFileName, AsyncIteratorFetcher<TripleID> source,
			MultiThreadListener listener, int bufferSize, long chunkSize, int k, TripleComponentOrder oldOrder,
			TripleComponentOrder newOrder) {
		this.source = source;
		this.listener = MultiThreadListener.ofNullable(listener);
		this.baseFileName = baseFileName;
		this.bufferSize = bufferSize;
		this.chunkSize = chunkSize;
		this.k = k;
		this.oldOrder = oldOrder;
		this.newOrder = newOrder;
	}

	@Override
	public void createChunk(SizeFetcher<TripleID> flux, CloseSuppressPath output)
			throws KWayMerger.KWayMergerException {
		ParallelSortableArrayList<TripleID> pairs = new ParallelSortableArrayList<>(TripleID[].class);

		TripleID tid;
		// loading the pairs
		listener.notifyProgress(10, "reading triple part 0");
		while ((tid = flux.get()) != null) {
			TripleOrderConvert.swapComponentOrder(tid, oldOrder, newOrder);
			pairs.add(tid);
			long r = read.incrementAndGet();
			if (r % 1_000_000 == 0) {
				listener.notifyProgress(10, "reading triple part " + r);
			}
		}

		// sort the pairs
		pairs.parallelSort(TripleID::compareTo);

		// write the result on disk
		int count = 0;
		int block = pairs.size() < 10 ? 1 : pairs.size() / 10;
		IntermediateListener il = new IntermediateListener(listener);
		il.setRange(70, 100);
		il.notifyProgress(0, "creating file");
		try (CompressTripleWriter w = new CompressTripleWriter(output.openOutputStream(bufferSize), false)) {
			// encode the size of the chunk
			for (int i = 0; i < pairs.size(); i++) {
				w.appendTriple(pairs.get(i));
				if (i % block == 0) {
					il.notifyProgress(i / (block / 10f), "writing triples " + count + "/" + pairs.size());
				}
			}
			listener.notifyProgress(100, "writing completed " + pairs.size() + " " + output.getFileName());
		} catch (IOException e) {
			throw new KWayMerger.KWayMergerException("Can't write chunk", e);
		}
	}

	@Override
	public void mergeChunks(List<CloseSuppressPath> inputs, CloseSuppressPath output)
			throws KWayMerger.KWayMergerException {
		try {
			listener.notifyProgress(0, "merging triples " + output.getFileName());
			CompressTripleReader[] readers = new CompressTripleReader[inputs.size()];
			long count = 0;
			try {
				for (int i = 0; i < inputs.size(); i++) {
					readers[i] = new CompressTripleReader(inputs.get(i).openInputStream(bufferSize));
				}

				// use spo because we are writing xyz
				ExceptionIterator<TripleID, IOException> it = CompressTripleMergeIterator.buildOfTree(readers,
						TripleComponentOrder.SPO);
				// at least one
				long rSize = it.getSize();
				long size = Math.max(rSize, 1);
				long block = size < 10 ? 1 : size / 10;
				try (CompressTripleWriter w = new CompressTripleWriter(output.openOutputStream(bufferSize), false)) {
					while (it.hasNext()) {
						w.appendTriple(it.next());
						if (count % block == 0) {
							listener.notifyProgress(count / (block / 10f), "merging triples " + count + "/" + size);
						}
						count++;
					}
				}
			} finally {
				IOUtil.closeAll(readers);
			}
			listener.notifyProgress(100, "triples merged " + output.getFileName() + " " + count);
			// delete old pairs
			IOUtil.closeAll(inputs);
		} catch (IOException e) {
			throw new KWayMerger.KWayMergerException(e);
		}
	}

	@Override
	public SizeFetcher<TripleID> newStopFlux(Supplier<TripleID> flux) {
		return SizeFetcher.of(flux, p -> 3 * Long.BYTES, chunkSize);
	}

	public ExceptionIterator<TripleID, IOException> sort(int workers)
			throws InterruptedException, IOException, KWayMerger.KWayMergerException {
		listener.notifyProgress(0, "Triple sort asked in " + baseFileName.toAbsolutePath());
		// force to create the first file
		KWayMerger<TripleID, SizeFetcher<TripleID>> merger = new KWayMerger<>(baseFileName, source, this,
				Math.max(1, workers - 1), k);
		merger.start();
		// wait for the workers to merge the sections and create the triples
		Optional<CloseSuppressPath> sections = merger.waitResult();
		if (sections.isEmpty()) {
			return ExceptionIterator.empty();
		}
		CloseSuppressPath path = sections.get();
		return new CompressTripleReader(path.openInputStream(bufferSize)) {
			@Override
			public void close() throws IOException {
				try {
					super.close();
				} finally {
					IOUtil.closeObject(path);
				}
			}
		};
	}
}

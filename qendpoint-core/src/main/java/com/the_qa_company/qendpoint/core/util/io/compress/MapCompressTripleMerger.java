package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressTripleMapper;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressionResult;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.TripleCompressionResult;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.TripleCompressionResultEmpty;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.TripleCompressionResultFile;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.TripleCompressionResultPartial;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleIDComparator;
import com.the_qa_company.qendpoint.core.util.ParallelSortableArrayList;
import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.SizeFetcher;
import com.the_qa_company.qendpoint.core.util.concurrent.KWayMerger;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * TreeWorkerObject implementation to map and merge tripleID from a compress
 * triple file
 *
 * @author Antoine Willerval
 */
public class MapCompressTripleMerger implements KWayMerger.KWayMergerImpl<TripleID, SizeFetcher<TripleID>> {
	private final CloseSuppressPath baseFileName;
	private final AsyncIteratorFetcher<TripleID> source;
	private final CompressTripleMapper mapper;
	private final MultiThreadListener listener;
	private final TripleComponentOrder order;
	private final int bufferSize;
	private final int k;
	private final AtomicLong triplesCount = new AtomicLong();
	private final long chunkSize;

	public MapCompressTripleMerger(CloseSuppressPath baseFileName, AsyncIteratorFetcher<TripleID> source,
			CompressTripleMapper mapper, MultiThreadListener listener, TripleComponentOrder order, int bufferSize,
			long chunkSize, int k) {
		this.baseFileName = baseFileName;
		this.source = source;
		this.mapper = mapper;
		this.listener = listener;
		this.order = order;
		this.bufferSize = bufferSize;
		this.chunkSize = chunkSize;
		this.k = k;
	}

	/**
	 * merge these triples into a file
	 *
	 * @param workers number of worker
	 * @return result
	 * @throws KWayMerger.KWayMergerException TreeWorker error
	 * @throws InterruptedException           thread interruption
	 * @throws IOException                    io error
	 */
	public TripleCompressionResult mergeToFile(int workers)
			throws InterruptedException, IOException, KWayMerger.KWayMergerException {
		// force to create the first file
		KWayMerger<TripleID, SizeFetcher<TripleID>> merger = new KWayMerger<>(baseFileName, source, this,
				Math.max(1, workers - 1), k);
		merger.start();
		// wait for the workers to merge the sections and create the triples
		Optional<CloseSuppressPath> sections = merger.waitResult();
		if (sections.isEmpty()) {
			return new TripleCompressionResultEmpty(order);
		}
		return new TripleCompressionResultFile(triplesCount.get(), sections.get(), order, bufferSize);
	}

	/**
	 * merge these triples while reading them, increase the memory usage
	 *
	 * @return result
	 * @throws IOException io error
	 */
	public TripleCompressionResult mergeToPartial() throws IOException, KWayMerger.KWayMergerException {
		List<CloseSuppressPath> files = new ArrayList<>();
		try {
			baseFileName.mkdirs();
			baseFileName.closeWithDeleteRecurse();
			long fileName = 0;
			while (!source.isEnd()) {
				CloseSuppressPath file = baseFileName.resolve("chunk#" + fileName++);
				createChunk(newStopFlux(source), file);
				files.add(file);
			}
		} catch (Throwable e) {
			try {
				throw e;
			} finally {
				try {
					IOUtil.closeAll(files);
				} finally {
					baseFileName.close();
				}
			}
		}
		return new TripleCompressionResultPartial(files, triplesCount.get(), order, bufferSize) {
			@Override
			public void close() throws IOException {
				try {
					super.close();
				} finally {
					baseFileName.close();
				}
			}
		};
	}

	/**
	 * merge the triples into a result
	 *
	 * @param workers number of workers (complete mode)
	 * @param mode    the mode of merging
	 * @return result
	 * @throws KWayMerger.KWayMergerException TreeWorker error (complete mode)
	 * @throws InterruptedException           thread interruption (complete
	 *                                        mode)
	 * @throws IOException                    io error
	 */
	public TripleCompressionResult merge(int workers, String mode)
			throws KWayMerger.KWayMergerException, InterruptedException, IOException {
		return switch (Objects.requireNonNullElse(mode, "")) {
			case "", CompressionResult.COMPRESSION_MODE_COMPLETE -> mergeToFile(workers);
			case CompressionResult.COMPRESSION_MODE_PARTIAL -> mergeToPartial();
			default -> throw new IllegalArgumentException("Unknown compression mode: " + mode);
		};
	}

	@Override
	public void createChunk(SizeFetcher<TripleID> flux, CloseSuppressPath output)
			throws KWayMerger.KWayMergerException {
		BufferedTriples buffer = new BufferedTriples();
		ParallelSortableArrayList<TripleID> tripleIDS = buffer.triples;
		listener.notifyProgress(10, "reading triples part2  " + triplesCount);
		TripleID next;
		boolean quad = mapper.supportsGraph();
		while ((next = flux.get()) != null) {
			TripleID mappedTriple;

			if (quad) {
				mappedTriple = new TripleID(mapper.extractSubject(next.getSubject()),
						mapper.extractPredicate(next.getPredicate()), mapper.extractObjects(next.getObject()),
						mapper.extractGraph(next.getGraph()));
			} else {
				mappedTriple = new TripleID(mapper.extractSubject(next.getSubject()),
						mapper.extractPredicate(next.getPredicate()), mapper.extractObjects(next.getObject()));
			}
			assert mappedTriple.isValid();
			tripleIDS.add(mappedTriple);
			long count = triplesCount.incrementAndGet();
			if (count % 100_000 == 0) {
				listener.notifyProgress(10, "reading triples part2 " + triplesCount);
			}
			if (tripleIDS.size() == Integer.MAX_VALUE - 6) {
				break;
			}
		}
		try {
			tripleIDS.parallelSort(TripleIDComparator.getComparator(order));
			int count = 0;
			int block = tripleIDS.size() < 10 ? 1 : tripleIDS.size() / 10;
			IntermediateListener il = new IntermediateListener(listener);
			il.setRange(70, 100);
			il.setPrefix("writing triples " + output.getFileName() + " ");
			try (CompressTripleWriter w = new CompressTripleWriter(output.openOutputStream(bufferSize), quad)) {
				il.notifyProgress(0, "creating file");
				TripleID prev = quad ? new TripleID(-1, -1, -1, -1) :  new TripleID(-1, -1, -1);
				for (TripleID triple : tripleIDS) {
					count++;
					if (count % block == 0) {
						il.notifyProgress(count / (block / 10f), "writing triples " + count + "/" + tripleIDS.size());
					}
					if (prev.match(triple)) {
						continue;
					}
					if (quad) {
						prev.setAll(triple.getSubject(), triple.getPredicate(), triple.getObject(), triple.getGraph());
					} else {
						prev.setAll(triple.getSubject(), triple.getPredicate(), triple.getObject());
					}
					w.appendTriple(triple);
				}
				listener.notifyProgress(100, "writing completed " + triplesCount + " " + output.getFileName());
			}
		} catch (IOException e) {
			throw new KWayMerger.KWayMergerException(e);
		}
	}

	@Override
	public void mergeChunks(List<CloseSuppressPath> inputs, CloseSuppressPath output)
			throws KWayMerger.KWayMergerException {
		try {
			listener.notifyProgress(0, "merging triples " + output.getFileName());
			CompressTripleReader[] readers = new CompressTripleReader[inputs.size()];
			try {
				for (int i = 0; i < inputs.size(); i++) {
					readers[i] = new CompressTripleReader(inputs.get(i).openInputStream(bufferSize));
				}

				try (CompressTripleWriter w = new CompressTripleWriter(output.openOutputStream(bufferSize), mapper.supportsGraph())) {
					ExceptionIterator<TripleID, IOException> it = CompressTripleMergeIterator.buildOfTree(readers,
							order);
					while (it.hasNext()) {
						w.appendTriple(it.next());
					}
				}
			} finally {
				IOUtil.closeAll(readers);
			}
			listener.notifyProgress(100, "triples merged " + output.getFileName());
			// delete old triples
			IOUtil.closeAll(inputs);
		} catch (IOException e) {
			throw new KWayMerger.KWayMergerException(e);
		}
	}

	@Override
	public SizeFetcher<TripleID> newStopFlux(Supplier<TripleID> flux) {
		return SizeFetcher.ofTripleLong(flux, chunkSize);
	}

	public static class BufferedTriples {
		ParallelSortableArrayList<TripleID> triples = new ParallelSortableArrayList<>(TripleID[].class);

		private BufferedTriples() {
		}
	}
}

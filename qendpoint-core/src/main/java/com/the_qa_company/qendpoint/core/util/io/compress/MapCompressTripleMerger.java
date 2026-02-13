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
import com.the_qa_company.qendpoint.core.iterator.utils.SizedSupplier;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionSupplier;
import com.the_qa_company.qendpoint.core.util.concurrent.KWayMerger;
import com.the_qa_company.qendpoint.core.util.concurrent.KWayMergerChunked;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * TreeWorkerObject implementation to map and merge tripleID from a compress
 * triple file
 *
 * @author Antoine Willerval
 */
public class MapCompressTripleMerger implements KWayMerger.KWayMergerImpl<TripleID, SizedSupplier<TripleID>>,
		KWayMergerChunked.KWayMergerChunkedImpl<TripleID, SizedSupplier<TripleID>> {
	private final CloseSuppressPath baseFileName;
	private final AsyncIteratorFetcher<TripleID> source; // may be null in
															// pull-mode
	private final CompressTripleMapper mapper;
	private final MultiThreadListener listener;
	private final TripleComponentOrder order;
	private final int bufferSize;
	private final int k;
	private final int maxConcurrentMerges;
	private final LongAdder triplesCount = new LongAdder();
	private final ConcurrentHashMap<CloseSuppressPath, Long> tripleCountByFile = new ConcurrentHashMap<>();
	private final long chunkSize;
	private final long graphs;

	public MapCompressTripleMerger(CloseSuppressPath baseFileName, AsyncIteratorFetcher<TripleID> source,
			CompressTripleMapper mapper, MultiThreadListener listener, TripleComponentOrder order, int bufferSize,
			long chunkSize, int k, long graphs) {
		this(baseFileName, source, mapper, listener, order, bufferSize, chunkSize, k, graphs, Integer.MAX_VALUE);
	}

	public MapCompressTripleMerger(CloseSuppressPath baseFileName, AsyncIteratorFetcher<TripleID> source,
			CompressTripleMapper mapper, MultiThreadListener listener, TripleComponentOrder order, int bufferSize,
			long chunkSize, int k, long graphs, int maxConcurrentMerges) {
		this.baseFileName = baseFileName;
		this.source = source;
		this.mapper = mapper;
		this.listener = listener;
		this.order = order;
		this.bufferSize = bufferSize;
		this.chunkSize = chunkSize;
		this.k = k;
		this.graphs = graphs;
		this.maxConcurrentMerges = maxConcurrentMerges <= 0 ? Integer.MAX_VALUE : maxConcurrentMerges;
	}

	public MapCompressTripleMerger(CloseSuppressPath baseFileName, CompressTripleMapper mapper,
			MultiThreadListener listener, TripleComponentOrder order, int bufferSize, long chunkSize, int k,
			long graphs) {
		this(baseFileName, null, mapper, listener, order, bufferSize, chunkSize, k, graphs, Integer.MAX_VALUE);
	}

	public MapCompressTripleMerger(CloseSuppressPath baseFileName, CompressTripleMapper mapper,
			MultiThreadListener listener, TripleComponentOrder order, int bufferSize, long chunkSize, int k,
			long graphs, int maxConcurrentMerges) {
		this(baseFileName, null, mapper, listener, order, bufferSize, chunkSize, k, graphs, maxConcurrentMerges);
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
		if (source == null) {
			throw new IllegalStateException("mergeToFile(workers) requires a source; use mergePull(...) instead.");
		}
		// force to create the first file
		int workerThreads = Math.max(1, workers - 1);
		int mergeLimit = Math.min(workerThreads, maxConcurrentMerges);
		KWayMerger<TripleID, SizedSupplier<TripleID>> merger = new KWayMerger<>(baseFileName, source, this,
				workerThreads, k, mergeLimit);
		merger.start();
		// wait for the workers to merge the sections and create the triples
		Optional<CloseSuppressPath> sections = merger.waitResult();
		if (sections.isEmpty()) {
			return new TripleCompressionResultEmpty(order);
		}
		long outputTripleCount = tripleCountByFile.getOrDefault(sections.get(), triplesCount.sum());
		return new TripleCompressionResultFile(outputTripleCount, sections.get(), order, bufferSize, graphs);
	}

	/**
	 * merge these triples while reading them, increase the memory usage
	 *
	 * @return result
	 * @throws IOException io error
	 */
	public TripleCompressionResult mergeToPartial() throws IOException, KWayMerger.KWayMergerException {
		if (source == null) {
			throw new IllegalStateException("mergeToPartial() requires a source; use mergePull(...) instead.");
		}
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
		long estimatedTripleCount = files.stream().mapToLong(f -> tripleCountByFile.getOrDefault(f, 0L)).sum();
		return new TripleCompressionResultPartial(files, estimatedTripleCount, order, bufferSize, graphs) {
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

	public TripleCompressionResult mergePull(int workers, String mode,
			ExceptionSupplier<SizedSupplier<TripleID>, IOException> chunkSupplier)
			throws KWayMerger.KWayMergerException, InterruptedException, IOException {
		if (mode == null) {
			mode = "";
		}
		return switch (mode) {
		case "", CompressionResult.COMPRESSION_MODE_COMPLETE -> mergeToFilePull(workers, chunkSupplier);
		case CompressionResult.COMPRESSION_MODE_PARTIAL -> mergeToPartialPull(chunkSupplier);
		default -> throw new IllegalArgumentException("Unknown compression mode: " + mode);
		};
	}

	public TripleCompressionResult mergeToFilePull(int workers,
			ExceptionSupplier<SizedSupplier<TripleID>, IOException> chunkSupplier)
			throws InterruptedException, IOException, KWayMerger.KWayMergerException {
		int workerThreads = Math.max(1, workers - 1);
		int mergeLimit = Math.min(workerThreads, maxConcurrentMerges);
		KWayMergerChunked<TripleID, SizedSupplier<TripleID>> merger = new KWayMergerChunked<>(baseFileName,
				chunkSupplier, this, workerThreads, k, mergeLimit);
		merger.start();
		Optional<CloseSuppressPath> sections = merger.waitResult();
		if (sections.isEmpty()) {
			return new TripleCompressionResultEmpty(order);
		}
		long outputTripleCount = tripleCountByFile.getOrDefault(sections.get(), triplesCount.sum());
		return new TripleCompressionResultFile(outputTripleCount, sections.get(), order, bufferSize, graphs);
	}

	public TripleCompressionResult mergeToPartialPull(
			ExceptionSupplier<SizedSupplier<TripleID>, IOException> chunkSupplier)
			throws IOException, KWayMerger.KWayMergerException {
		List<CloseSuppressPath> files = new ArrayList<>();
		baseFileName.closeWithDeleteRecurse();
		try {
			baseFileName.mkdirs();
			baseFileName.closeWithDeleteRecurse();
			long fileName = 0;

			while (true) {
				SizedSupplier<TripleID> chunk = chunkSupplier.get();
				if (chunk == null) {
					break;
				}
				CloseSuppressPath file = baseFileName.resolve("chunk#" + fileName++);
				createChunk(chunk, file);
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
		long estimatedTripleCount = files.stream().mapToLong(f -> tripleCountByFile.getOrDefault(f, 0L)).sum();
		return new TripleCompressionResultPartial(files, estimatedTripleCount, order, bufferSize, graphs) {
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

	long localCount = 0;

	@Override
	public void createChunk(SizedSupplier<TripleID> flux, CloseSuppressPath output)
			throws KWayMerger.KWayMergerException {
		long elementSize = mapper.supportsGraph() ? 4L * Long.BYTES : 3L * Long.BYTES;
		int expectedCapacity = (int) Math.min(Integer.MAX_VALUE - 5L, Math.max(16L, chunkSize / elementSize));
		BufferedTriples buffer = new BufferedTriples(expectedCapacity);
		ParallelSortableArrayList<TripleID> tripleIDS = buffer.triples;
		listener.notifyProgress(10, "reading triples part2 " + triplesCount.sum());
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
			triplesCount.increment();
			long l = localCount++;
			if (l % 10_000 == 0) {
				long count = triplesCount.sum();
				listener.notifyProgress(10, "reading triples part2 " + count);
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
				TripleID prev = quad ? new TripleID(-1, -1, -1, -1) : new TripleID(-1, -1, -1);
				long written = 0;
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
					written++;
				}
				tripleCountByFile.put(output, written);
				listener.notifyProgress(100, "writing completed " + triplesCount.sum() + " " + output.getFileName());
			}
		} catch (IOException e) {
			throw new KWayMerger.KWayMergerException(e);
		}
	}

	@Override
	public void mergeChunks(List<CloseSuppressPath> inputs, CloseSuppressPath output)
			throws KWayMerger.KWayMergerException {
		try {
			if (Thread.currentThread().isInterrupted()) {
				throw new RuntimeException("Thread interrupted before merging triples");
			}

			listener.notifyProgress(0, "merging triples " + output.getFileName());
			CompressTripleReader[] readers = new CompressTripleReader[inputs.size()];
			try {
				for (int i = 0; i < inputs.size(); i++) {
					readers[i] = new CompressTripleReader(inputs.get(i).openInputStream(bufferSize));
				}

				try (CompressTripleWriter w = new CompressTripleWriter(output.openOutputStream(bufferSize),
						mapper.supportsGraph())) {
					ExceptionIterator<TripleID, IOException> it = CompressTripleMergeIterator.buildOfTree(readers,
							order);
					long count = 0;
					long size = Math.max(it.getSize(), 1);
					long block = size < 10 ? 1 : size / 10;
					long written = 0;
					while (it.hasNext()) {
						w.appendTriple(it.next());
						if (count % 1_000_000 == 0) {
							listener.notifyProgress(count / (block / 10f), "merging triples " + count + "/" + size);
						}
						count++;
						written++;
					}
					tripleCountByFile.put(output, written);
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
	public SizedSupplier<TripleID> newStopFlux(Supplier<TripleID> flux) {
		long elementSize = mapper.supportsGraph() ? 4L * Long.BYTES : 3L * Long.BYTES;
		return SizeFetcher.of(flux, ignored -> elementSize, chunkSize);
	}

	public static class BufferedTriples {
		ParallelSortableArrayList<TripleID> triples;

		private BufferedTriples(int initialCapacity) {
			triples = new ParallelSortableArrayList<>(TripleID[].class, initialCapacity);
		}
	}
}

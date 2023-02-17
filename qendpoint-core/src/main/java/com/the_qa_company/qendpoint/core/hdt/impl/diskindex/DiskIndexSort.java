package com.the_qa_company.qendpoint.core.hdt.impl.diskindex;

import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.SizeFetcher;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.util.ParallelSortableArrayList;
import com.the_qa_company.qendpoint.core.util.concurrent.KWayMerger;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.Pair;
import com.the_qa_company.qendpoint.core.util.io.compress.PairMergeIterator;
import com.the_qa_company.qendpoint.core.util.io.compress.PairReader;
import com.the_qa_company.qendpoint.core.util.io.compress.PairWriter;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Implementation to use K-Way merge sort to create Object index
 *
 * @author Antoine Willerval
 */
public class DiskIndexSort implements KWayMerger.KWayMergerImpl<Pair, SizeFetcher<Pair>> {
	private final CloseSuppressPath baseFileName;
	private final AsyncIteratorFetcher<Pair> source;
	private final MultiThreadListener listener;
	private final int bufferSize;
	private final long chunkSize;
	private final int k;
	private final Comparator<Pair> comparator;
	private final AtomicLong read = new AtomicLong();

	public DiskIndexSort(CloseSuppressPath baseFileName, AsyncIteratorFetcher<Pair> source,
			MultiThreadListener listener, int bufferSize, long chunkSize, int k, Comparator<Pair> comparator) {
		this.source = source;
		this.listener = MultiThreadListener.ofNullable(listener);
		this.baseFileName = baseFileName;
		this.bufferSize = bufferSize;
		this.chunkSize = chunkSize;
		this.k = k;
		this.comparator = comparator;
	}

	@Override
	public void createChunk(SizeFetcher<Pair> flux, CloseSuppressPath output) throws KWayMerger.KWayMergerException {
		ParallelSortableArrayList<Pair> pairs = new ParallelSortableArrayList<>(Pair[].class);

		Pair pair;
		// loading the pairs
		listener.notifyProgress(10, "reading pairs part 0");
		while ((pair = flux.get()) != null) {
			pairs.add(pair);
			long r = read.incrementAndGet();
			if (r % 1_000_000 == 0) {
				listener.notifyProgress(10, "reading pairs part " + r);
			}
		}

		// sort the pairs
		pairs.parallelSort(comparator);

		// write the result on disk
		int count = 0;
		int block = pairs.size() < 10 ? 1 : pairs.size() / 10;
		IntermediateListener il = new IntermediateListener(listener);
		il.setRange(70, 100);
		il.notifyProgress(0, "creating file");
		try (PairWriter w = new PairWriter(output.openOutputStream(bufferSize), pairs.size())) {
			// encode the size of the chunk
			for (int i = 0; i < pairs.size(); i++) {
				if (i % block == 0) {
					il.notifyProgress(i / (block / 10f), "writing pair " + count + "/" + pairs.size());
				}
				w.append(pairs.get(i));
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
			listener.notifyProgress(0, "merging pairs " + output.getFileName());
			PairReader[] readers = new PairReader[inputs.size()];
			long count = 0;
			try {
				for (int i = 0; i < inputs.size(); i++) {
					readers[i] = new PairReader(inputs.get(i).openInputStream(bufferSize));
				}

				ExceptionIterator<Pair, IOException> it = PairMergeIterator.buildOfTree(readers, comparator);
				// at least one
				long rSize = it.getSize();
				long size = Math.max(rSize, 1);
				long block = size < 10 ? 1 : size / 10;
				try (PairWriter w = new PairWriter(output.openOutputStream(bufferSize), rSize)) {
					while (it.hasNext()) {
						w.append(it.next());
						if (count % block == 0) {
							listener.notifyProgress(count / (block / 10f), "merging pairs " + count + "/" + size);
						}
						count++;
					}
				}
			} finally {
				IOUtil.closeAll(readers);
			}
			listener.notifyProgress(100, "pairs merged " + output.getFileName() + " " + count);
			// delete old pairs
			IOUtil.closeAll(inputs);
		} catch (IOException e) {
			throw new KWayMerger.KWayMergerException(e);
		}
	}

	@Override
	public SizeFetcher<Pair> newStopFlux(Supplier<Pair> flux) {
		return SizeFetcher.of(flux, p -> 3 * Long.BYTES, chunkSize);
	}

	/**
	 * sort the pairs
	 *
	 * @param workers number of workers to handle the kway merge
	 * @return exception iterator, might implement {@link java.io.Closeable},
	 *         use {@link IOUtil#closeObject(Object)} if required
	 * @throws InterruptedException           thread interruption
	 * @throws IOException                    io exception
	 * @throws KWayMerger.KWayMergerException exception during the kway merge
	 */
	public ExceptionIterator<Pair, IOException> sort(int workers)
			throws InterruptedException, IOException, KWayMerger.KWayMergerException {
		listener.notifyProgress(0, "Pair sort asked in " + baseFileName.toAbsolutePath());
		// force to create the first file
		KWayMerger<Pair, SizeFetcher<Pair>> merger = new KWayMerger<>(baseFileName, source, this,
				Math.max(1, workers - 1), k);
		merger.start();
		// wait for the workers to merge the sections and create the triples
		Optional<CloseSuppressPath> sections = merger.waitResult();
		if (sections.isEmpty()) {
			return ExceptionIterator.empty();
		}
		CloseSuppressPath path = sections.get();
		return new PairReader(path.openInputStream(bufferSize)) {
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

package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64Big;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MergeExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.SizeFetcher;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.concurrent.KWayMerger;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.Closer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Ids sorter for the {@link QEPMap} linking process
 *
 * @author Antoine Willerval
 */
public class QEPMapIdSorter implements Closeable, Iterable<QEPMapIdSorter.QEPMapIds> {

	public record QEPMapIds(long origin, long destination) implements Comparable<QEPMapIds> {
		@Override
		public int compareTo(QEPMapIds o) {
			return Long.compare(origin, o.origin);
		}
	}

	public static final long MAX_ELEMENT_SIZE_THRESHOLD = 500_000_000L; // max 500MB
	private final LongArray ids;
	private long index;
	private final CloseSuppressPath computeLocation;

	public QEPMapIdSorter(Path computeLocation, long maxElementCount, long maxValue) {
		this.computeLocation = CloseSuppressPath.of(computeLocation);
		this.computeLocation.closeWithDeleteRecurse();
		int bits = BitUtil.log2(maxValue);
		if (maxElementCount * bits * 2 / 8 < MAX_ELEMENT_SIZE_THRESHOLD) {
			ids = new SequenceLog64Big(bits, maxElementCount << 1);
		} else {
			ids = new SequenceLog64BigDisk(computeLocation, bits, maxElementCount * 2);
		}
	}

	/**
	 * add a new mapping to the sorter
	 *
	 * @param origin      origin id
	 * @param destination destination id
	 */
	public void addElement(long origin, long destination) {
		ids.set((index << 1), origin);
		ids.set((index << 1) | 1, destination);
		index++;
	}

	/**
	 * sort the ids
	 *
	 * @throws IOException io exception
	 */
	public void sort() throws IOException {
		try (CloseSuppressPath arrSort = computeLocation.resolveSibling(computeLocation.getFileName() + "-arrsort")) {
			arrSort.closeWithDeleteRecurse();
			arrSort.mkdirs();

			try {
				Runtime runtime = Runtime.getRuntime();
				int workers = runtime.availableProcessors();
				long chunkSize = (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) / (0.85 * workers));
				KWayMerger<QEPMapIds, Supplier<QEPMapIds>> merger = new KWayMerger<>(
						arrSort, new AsyncIteratorFetcher<>(new IdSupplier()), new Merger(chunkSize),
						workers, 16
				);
				merger.start();

				CloseSuppressPath output = merger.waitResult().orElse(null);

				if (output != null) {
					try (BufferedInputStream stream = new BufferedInputStream(Files.newInputStream(output))) {
						QEPMapReader reader = new QEPMapReader(stream);

						long index = 0;
						while (reader.hasNext()) {
							QEPMapIds next = reader.next();
							ids.set(index++, next.origin());
							ids.set(index++, next.destination());
						}
					}
				}
			} catch (KWayMerger.KWayMergerException | InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	/**
	 * @return the number of added elements
	 */
	public long size() {
		return index;
	}

	/**
	 * fetch a mapping
	 *
	 * @param index index of the mapping
	 * @return ids
	 */
	public QEPMapIds get(long index) {
		return new QEPMapIds(ids.get(index * 2), ids.get(index * 2 + 1));
	}

	@Override
	public void close() throws IOException {
		Closer.closeAll(ids, computeLocation);
	}

	@Override
	public Iterator<QEPMapIds> iterator() {
		return new IdIterator();
	}

	private class IdIterator extends FetcherIterator<QEPMapIds> {
		long itIndex;

		@Override
		protected QEPMapIds getNext() {
			if (itIndex < size()) {
				return get(itIndex++);
			} else {
				return null;
			}
		}
	}


	private record Merger(long chunkSize) implements KWayMerger.KWayMergerImpl<QEPMapIds, Supplier<QEPMapIds>> {

		@Override
		public void createChunk(Supplier<QEPMapIds> flux, CloseSuppressPath output) throws KWayMerger.KWayMergerException {
			try (BufferedOutputStream stream = new BufferedOutputStream(Files.newOutputStream(output))) {
				QEPMapIds ids;

				List<QEPMapIds> idList = new ArrayList<>();

				while ((ids = flux.get()) != null) {
					idList.add(ids);
				}

				idList.sort(QEPMapIds::compareTo);

				for (QEPMapIds qepMapIds : idList) {
					VByte.encode(stream, qepMapIds.origin());
					VByte.encode(stream, qepMapIds.destination());
				}

				VByte.encode(stream, 0);
				VByte.encode(stream, 0);
			} catch (IOException e) {
				throw new KWayMerger.KWayMergerException(e);
			}
		}

		@Override
		public void mergeChunks(List<CloseSuppressPath> inputs, CloseSuppressPath output) throws KWayMerger.KWayMergerException {
			try {
				InputStream[] pathInput = new InputStream[inputs.size()];

				for (int i = 0; i < pathInput.length; i++) {
					pathInput[i] = new BufferedInputStream(Files.newInputStream(inputs.get(i)));
				}

				try {

					ExceptionIterator<QEPMapIds, IOException> tree = MergeExceptionIterator.buildOfTree(QEPMapReader::new, Arrays.asList(pathInput), 0, inputs.size());

					try (BufferedOutputStream stream = new BufferedOutputStream(Files.newOutputStream(output))) {
						while (tree.hasNext()) {
							QEPMapIds ids = tree.next();
							VByte.encode(stream, ids.origin());
							VByte.encode(stream, ids.destination());
						}
						VByte.encode(stream, 0);
						VByte.encode(stream, 0);
					}
				} finally {
					Closer.closeAll(inputs);
				}
			} catch (IOException e) {
				throw new KWayMerger.KWayMergerException(e);
			}
		}

		@Override
		public Supplier<QEPMapIds> newStopFlux(Supplier<QEPMapIds> flux) {
			return SizeFetcher.of(flux, p -> 2 * Long.BYTES, chunkSize);
		}
	}

	private static class QEPMapReader extends FetcherExceptionIterator<QEPMapIds, IOException> {
		private final InputStream stream;

		private QEPMapReader(InputStream stream) {
			this.stream = stream;
		}

		@Override
		protected QEPMapIds getNext() throws IOException {
			long origin = VByte.decode(stream);
			long destination = VByte.decode(stream);
			if (origin == 0 || destination == 0) {
				if (origin != 0 || destination != 0) {
					throw new IOException("bad reading: " + origin + "/" + destination);
				}
				return null;
			}
			return new QEPMapIds(origin, destination);
		}
	}

	private class IdSupplier extends FetcherIterator<QEPMapIds> {
		long i;

		@Override
		protected QEPMapIds getNext() {
			if (i >= index) {
				return null;
			}

			long origin = ids.get(i * 2);
			long destination = ids.get(i * 2 + 1);
			i++;
			return new QEPMapIds(origin, destination);
		}
	}
}

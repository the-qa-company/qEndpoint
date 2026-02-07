/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples/impl/BitmapTriples.java
 * $ Revision: $Rev: 203 $ Last modified: $Date: 2013-05-24 10:48:53 +0100 (vie,
 * 24 may 2013) $ Last modified by: $Author: mario.arias $ This library is free
 * software; you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License. This library is distributed in the hope that it
 * will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.AdjacencyList;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap375Big;
import com.the_qa_company.qendpoint.core.compact.bitmap.BitmapFactory;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.MultiLayerBitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceFactory;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64Big;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.SignatureIOException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTDiskImporter;
import com.the_qa_company.qendpoint.core.hdt.impl.diskindex.DiskIndexSort;
import com.the_qa_company.qendpoint.core.hdt.impl.diskindex.ObjectAdjReader;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.iterator.SequentialSearchIteratorTripleID;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.IteratorChunkedSource;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.concurrent.KWayMerger;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.Pair;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;
import org.apache.commons.io.file.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author mario.arias
 */
public class BitmapTriples implements TriplesPrivate, BitmapTriplesIndex {
	private static final Logger log = LoggerFactory.getLogger(BitmapTriples.class);
	private static final int OBJECT_INDEX_PARALLEL_FILL_MIN_RECORDS = 1 << 15;
	private static final int OBJECT_INDEX_PARALLEL_FILL_MIN_GROUPS = 4;
	private static final long OBJECT_INDEX_PROGRESS_MIN_INTERVAL_MILLIS = 500L;
	public static int REPORT_INTERVAL = 100_000;

	protected TripleComponentOrder order;

	protected final Map<TripleComponentOrder, BitmapTriplesIndex> indexes = new HashMap<>();
	protected int indexesMask = 0;

	protected Sequence seqY, seqZ, indexZ, predicateCount;
	protected Bitmap bitmapY, bitmapZ, bitmapIndexZ;

	protected AdjacencyList adjY, adjZ, adjIndex;

	// Index for Y
	public PredicateIndex predicateIndex;

	boolean diskSequence;
	boolean diskSubIndex;
	CreateOnUsePath diskSequenceLocation;

	protected boolean isClosed;
	private int objectIndexParallelism = Runtime.getRuntime().availableProcessors();
	private boolean objectIndexPipelineEnabled;
	private boolean objectIndexBucketParallelWritesEnabled;
	private boolean objectIndexBatchParallelFillEnabled;
	private boolean objectIndexBatchProcessingPipelineEnabled;

	public BitmapTriples() throws IOException {
		this(new HDTSpecification());
	}

	public BitmapTriples(HDTOptions spec) throws IOException {
		String orderStr = spec.get(HDTOptionsKeys.TRIPLE_ORDER_KEY);
		if (orderStr == null) {
			this.order = TripleComponentOrder.SPO;
		} else {
			this.order = TripleComponentOrder.valueOf(orderStr);
		}

		loadDiskSequence(spec);

		bitmapY = BitmapFactory.createBitmap(spec.get(HDTOptionsKeys.BITMAPTRIPLES_BITMAP_Y));
		bitmapZ = BitmapFactory.createBitmap(spec.get(HDTOptionsKeys.BITMAPTRIPLES_BITMAP_Z));

		seqY = SequenceFactory.createStream(spec.get(HDTOptionsKeys.BITMAPTRIPLES_SEQ_Y));
		seqZ = SequenceFactory.createStream(spec.get(HDTOptionsKeys.BITMAPTRIPLES_SEQ_Z));

		adjY = new AdjacencyList(seqY, bitmapY);
		adjZ = new AdjacencyList(seqZ, bitmapZ);

		isClosed = false;
	}

	public BitmapTriples(HDTOptions spec, Sequence seqY, Sequence seqZ, Bitmap bitY, Bitmap bitZ,
			TripleComponentOrder order) throws IOException {
		this.seqY = seqY;
		this.seqZ = seqZ;
		this.bitmapY = bitY;
		this.bitmapZ = bitZ;
		this.order = order;

		loadDiskSequence(spec);

		adjY = new AdjacencyList(seqY, bitmapY);
		adjZ = new AdjacencyList(seqZ, bitmapZ);

		isClosed = false;
	}

	private void loadDiskSequence(HDTOptions spec) throws IOException {
		diskSequence = spec != null && spec.getBoolean(HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK, false);
		diskSubIndex = spec != null && spec.getBoolean(HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_SUBINDEX, false);

		if (diskSequenceLocation != null) {
			diskSequenceLocation.close();
		}

		if (diskSequence) {
			assert spec != null; // happy compiler
			String optDiskLocation = spec.get(HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_LOCATION);
			if (optDiskLocation != null && !optDiskLocation.isEmpty()) {
				diskSequenceLocation = new CreateOnUsePath(Path.of(optDiskLocation));
			} else {
				diskSequenceLocation = new CreateOnUsePath();
			}
		} else {
			diskSequenceLocation = null;
		}
	}

	private int resolveObjectIndexParallelism(HDTOptions spec) {
		if (spec == null) {
			return 1;
		}
		long configured = spec.getInt(HDTOptionsKeys.BITMAPTRIPLES_OBJECT_INDEX_PARALLELISM_KEY,
				Runtime.getRuntime()::availableProcessors);
		if (configured == 0) {
			configured = Runtime.getRuntime().availableProcessors();
		}
		if (configured < 0 || configured >= Integer.MAX_VALUE - 5L) {
			throw new IllegalArgumentException("Invalid value for "
					+ HDTOptionsKeys.BITMAPTRIPLES_OBJECT_INDEX_PARALLELISM_KEY + ": " + configured);
		}
		return (int) Math.max(1L, configured);
	}

	public CreateOnUsePath getDiskSequenceLocation() {
		return diskSequenceLocation;
	}

	public boolean isUsingDiskSequence() {
		return diskSequence;
	}

	public PredicateIndex getPredicateIndex() {
		return predicateIndex;
	}

	public Sequence getPredicateCount() {
		return predicateCount;
	}

	public void load(IteratorTripleID it, ProgressListener listener) {

		long number = it.estimatedNumResults();

		DynamicSequence vectorY = new SequenceLog64Big(BitUtil.log2(number), number + 1);
		DynamicSequence vectorZ = new SequenceLog64Big(BitUtil.log2(number), number + 1);

		ModifiableBitmap bitY = Bitmap375Big.memory(number);
		ModifiableBitmap bitZ = Bitmap375Big.memory(number);

		long lastX = 0, lastY = 0, lastZ = 0;
		long x, y, z;
		long numTriples = 0;

		while (it.hasNext()) {
			TripleID triple = it.next();
			TripleOrderConvert.swapComponentOrder(triple, TripleComponentOrder.SPO, order);

			x = triple.getSubject();
			y = triple.getPredicate();
			z = triple.getObject();
			if (x == 0 || y == 0 || z == 0) {
				throw new IllegalFormatException("None of the components of a triple can be null");
			}

			if (numTriples == 0) {
				// First triple
				vectorY.append(y);
				vectorZ.append(z);
			} else if (x != lastX) {
				if (x != lastX + 1) {
					throw new IllegalFormatException("Upper level must be increasing and correlative.");
				}
				// X changed
				bitY.append(true);
				vectorY.append(y);

				bitZ.append(true);
				vectorZ.append(z);
			} else if (y != lastY) {
				if (y < lastY) {
					throw new IllegalFormatException("Middle level must be increasing for each parent.");
				}

				// Y changed
				bitY.append(false);
				vectorY.append(y);

				bitZ.append(true);
				vectorZ.append(z);
			} else {
				if (z < lastZ) {
					throw new IllegalFormatException("Lower level must be increasing for each parent.");
				}

				// Z changed
				bitZ.append(false);
				vectorZ.append(z);
			}

			lastX = x;
			lastY = y;
			lastZ = z;

			ListenerUtil.notifyCond(listener, "Converting to BitmapTriples", numTriples, numTriples, number);
			numTriples++;
		}

		if (numTriples > 0) {
			bitY.append(true);
			bitZ.append(true);
		}

		vectorY.aggressiveTrimToSize();
		vectorZ.trimToSize();

		// Assign local variables to BitmapTriples Object
		seqY = vectorY;
		seqZ = vectorZ;
		bitmapY = bitY;
		bitmapZ = bitZ;

		adjY = new AdjacencyList(seqY, bitmapY);
		adjZ = new AdjacencyList(seqZ, bitmapZ);

		// DEBUG
//		adjY.dump();
//		adjZ.dump();

		isClosed = false;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#load(hdt.triples.TempTriples,
	 * hdt.ProgressListener)
	 */
	@Override
	public void load(TempTriples triples, ProgressListener listener) {
		triples.setOrder(order);
		triples.sort(listener);

		IteratorTripleID it = triples.searchAll();
		this.load(it, listener);
	}

	@Override
	public SuppliableIteratorTripleID search(TripleID pattern) {
		return search(pattern, TripleComponentOrder.ALL_MASK);
	}

	@Override
	public SuppliableIteratorTripleID search(TripleID pattern, int searchMask) {
		if (isClosed) {
			throw new IllegalStateException("Cannot search on BitmapTriples if it's already closed");
		}

		if (getNumberOfElements() == 0 || pattern.isNoMatch()) {
			return new EmptyTriplesIterator(order);
		}

		TripleID reorderedPat = new TripleID(pattern);
		TripleOrderConvert.swapComponentOrder(reorderedPat, TripleComponentOrder.SPO, order);
		int flags = reorderedPat.getPatternOrderFlags();

		if ((flags & searchMask & this.order.mask) != 0) {
			// we can use the default order, so we use it
			return search(pattern, this);
		}

		if ((indexesMask & flags) != 0) {
			BitmapTriplesIndex idx;

			int bestOrders = flags & searchMask;

			if ((indexesMask & bestOrders) != 0) {
				// we can use the asked order
				idx = TripleComponentOrder.fetchBestForCfg(bestOrders, indexes);
			} else {
				// no asked order found, we can still use the best index
				idx = TripleComponentOrder.fetchBestForCfg(flags, indexes);
			}

			assert idx != null : String.format("the tid flags were describing an unknown pattern: %x &= %x", flags,
					indexesMask & flags);

			return search(pattern, idx);
		}

		String patternString = reorderedPat.getPatternString();

		if (patternString.equals("?P?")) {
			if (this.predicateIndex != null) {
				return new BitmapTriplesIteratorYFOQ(this, pattern);
			} else {
				return new BitmapTriplesIteratorY(this, pattern);
			}
		}

		if (hasFOQIndex()) {
			// USE FOQ
			if (patternString.equals("?PO") || patternString.equals("??O")) {
				return new BitmapTriplesIteratorZFOQ(this, pattern);
			}
		} else {
			if (patternString.equals("?PO")) {
				return new SequentialSearchIteratorTripleID(pattern, new BitmapTriplesIteratorZ(this, pattern));
			}

			if (patternString.equals("??O")) {
				return new BitmapTriplesIteratorZ(this, pattern);
			}
		}

		SuppliableIteratorTripleID bitIt = new BitmapTriplesIterator(this, pattern);
		if (patternString.equals("???") || patternString.equals("S??") || patternString.equals("SP?")
				|| patternString.equals("SPO")) {
			return bitIt;
		} else {
			return new SequentialSearchIteratorTripleID(pattern, bitIt);
		}

	}

	public SuppliableIteratorTripleID search(TripleID pattern, BitmapTriplesIndex idx) {
		return new BitmapTriplesIterator(idx, pattern);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#searchAll()
	 */
	@Override
	public IteratorTripleID searchAll() {
		return searchAll(TripleComponentOrder.ALL_MASK);
	}

	@Override
	public IteratorTripleID searchAll(int searchMask) {
		return this.search(new TripleID(), searchMask);
	}

	public IteratorTripleID searchAll(BitmapTriplesIndex idx) {
		return this.search(new TripleID(), idx);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#getNumberOfElements()
	 */
	@Override
	public long getNumberOfElements() {
		if (isClosed) {
			return 0;
		}
		return seqZ.getNumberOfElements();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#size()
	 */
	@Override
	public long size() {
		if (isClosed) {
			return 0;
		}
		return seqY.size() + seqZ.size() + bitmapY.getSizeBytes() + bitmapZ.getSizeBytes();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#save(java.io.OutputStream, hdt.ControlInfo,
	 * hdt.ProgressListener)
	 */
	@Override
	public void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		ci.clear();
		ci.setFormat(getType());
		ci.setInt("order", order.ordinal());
		ci.setType(ControlInfo.Type.TRIPLES);
		ci.save(output);

		IntermediateListener iListener = new IntermediateListener(listener);
		bitmapY.save(output, iListener);
		bitmapZ.save(output, iListener);
		seqY.save(output, iListener);
		seqZ.save(output, iListener);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#load(java.io.InputStream, hdt.ControlInfo,
	 * hdt.ProgressListener)
	 */
	@Override
	public void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {

		if (ci.getType() != ControlInfo.Type.TRIPLES) {
			throw new IllegalFormatException("Trying to read a triples section, but was not triples.");
		}

		if (!ci.getFormat().equals(getType())) {
			throw new IllegalFormatException(
					"Trying to read BitmapTriples, but the data does not seem to be BitmapTriples");
		}

		order = TripleComponentOrder.values()[(int) ci.getInt("order")];

		IntermediateListener iListener = new IntermediateListener(listener);

		bitmapY = BitmapFactory.createBitmap(input);
		bitmapY.load(input, iListener);

		bitmapZ = BitmapFactory.createBitmap(input);
		bitmapZ.load(input, iListener);

		seqY = SequenceFactory.createStream(input);
		seqY.load(input, iListener);

		seqZ = SequenceFactory.createStream(input);
		seqZ.load(input, iListener);

		adjY = new AdjacencyList(seqY, bitmapY);
		adjZ = new AdjacencyList(seqZ, bitmapZ);

		isClosed = false;
	}

	@Override
	public void mapFromFile(CountInputStream input, File f, ProgressListener listener) throws IOException {
		log.trace("Mapping BitmapTriples from {}", f);
		ControlInformation ci = new ControlInformation();
		ci.load(input);
		if (ci.getType() != ControlInfo.Type.TRIPLES) {
			throw new IllegalFormatException("Trying to read a triples section, but was not triples.");
		}

		if (!ci.getFormat().equals(getType())) {
			throw new IllegalFormatException(
					"Trying to read BitmapTriples, but the data does not seem to be BitmapTriples");
		}

		order = TripleComponentOrder.values()[(int) ci.getInt("order")];

		IntermediateListener iListener = new IntermediateListener(listener);

		input.printIndex("bitmapY");
		bitmapY = BitmapFactory.createBitmap(input);
		bitmapY.load(input, iListener);

		input.printIndex("bitmapZ");
		bitmapZ = BitmapFactory.createBitmap(input);
		bitmapZ.load(input, iListener);

		input.printIndex("seqY");
		seqY = SequenceFactory.createStream(input, f);
		input.printIndex("seqZ");
		seqZ = SequenceFactory.createStream(input, f);

		adjY = new AdjacencyList(seqY, bitmapY);
		adjZ = new AdjacencyList(seqZ, bitmapZ);
		input.printIndex("end");

		isClosed = false;
	}

	public DynamicSequence createSequence64(Path baseDir, String name, int size, long capacity, boolean forceDisk)
			throws IOException {
		if (forceDisk && !diskSequence) {
			Path path = Files.createTempFile(name, ".bin");
			return new SequenceLog64BigDisk(path, size, capacity, true) {
				@Override
				public void close() throws IOException {
					try {
						super.close();
					} finally {
						Files.deleteIfExists(path);
					}
				}
			};
		} else {
			return createSequence64(baseDir, name, size, capacity);
		}
	}

	public DynamicSequence createSequence64(Path baseDir, String name, int size, long capacity) {
		if (diskSequence) {
			Path path = baseDir.resolve(name);
			return new SequenceLog64BigDisk(path, size, capacity, true) {
				@Override
				public void close() throws IOException {
					try {
						super.close();
					} finally {
						Files.deleteIfExists(path);
					}
				}
			};
		} else {
			return new SequenceLog64Big(size, capacity, true);
		}
	}

	public Bitmap375Big createBitmap375(Path baseDir, String name, long size) {
		if (diskSequence) {
			Path path = baseDir.resolve(name);
			Bitmap375Big bm = Bitmap375Big.disk(path, size, diskSubIndex);
			bm.getCloser().with(CloseSuppressPath.of(path));
			return bm;
		} else {
			return Bitmap375Big.memory(size);
		}
	}

	static long getMaxChunkSizeDiskIndex(int workers) {
		return (long) (HDTDiskImporter.getAvailableMemory() * 0.85 / (3L * workers));
	}

	private void createIndexObjectDisk(HDTOptions spec, Dictionary dictionary, ProgressListener plistener)
			throws IOException {
		MultiThreadListener listener = ListenerUtil.multiThreadListener(plistener);
		StopWatch global = new StopWatch();
		// load the config
		Path diskLocation;
		if (diskSequence) {
			diskLocation = diskSequenceLocation.createOrGetPath();
		} else {
			diskLocation = Files.createTempDirectory("bitmapTriples");
		}
		int workers = (int) spec.getInt(HDTOptionsKeys.BITMAPTRIPLES_DISK_WORKER_KEY,
				Runtime.getRuntime()::availableProcessors);
		// check and set default values if required
		if (workers <= 0) {
			throw new IllegalArgumentException("Number of workers should be positive!");
		}
		long chunkSize = spec.getInt(HDTOptionsKeys.BITMAPTRIPLES_DISK_CHUNK_SIZE_KEY,
				() -> getMaxChunkSizeDiskIndex(workers));
		if (chunkSize < 0) {
			throw new IllegalArgumentException("Negative chunk size!");
		}
		long maxFileOpenedLong = spec.getInt(HDTOptionsKeys.BITMAPTRIPLES_DISK_MAX_FILE_OPEN_KEY, 1024);
		int maxFileOpened;
		if (maxFileOpenedLong < 0 || maxFileOpenedLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("maxFileOpened should be positive!");
		} else {
			maxFileOpened = (int) maxFileOpenedLong;
		}
		long kwayLong = spec.getInt(HDTOptionsKeys.BITMAPTRIPLES_DISK_KWAY_KEY,
				() -> Math.max(1, BitUtil.log2(maxFileOpened / workers)));
		int k;
		if (kwayLong <= 0 || kwayLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("kway can't be negative!");
		} else {
			k = 1 << ((int) kwayLong);
		}
		long bufferSizeLong = spec.getInt(HDTOptionsKeys.BITMAPTRIPLES_DISK_BUFFER_SIZE_KEY,
				CloseSuppressPath.BUFFER_SIZE);
		int bufferSize;
		if (bufferSizeLong > Integer.MAX_VALUE - 5L || bufferSizeLong <= 0) {
			throw new IllegalArgumentException("Buffer size can't be negative or bigger than the size of an array!");
		} else {
			bufferSize = (int) bufferSizeLong;
		}

		// start the indexing
		DiskIndexSort sort = new DiskIndexSort(CloseSuppressPath.of(diskLocation).resolve("chunks"), listener,
				bufferSize, chunkSize, k,
				Comparator.<Pair>comparingLong(p -> p.object).thenComparingLong(p -> p.predicate));

		// Serialize
		DynamicSequence indexZ = null;
		ModifiableBitmap bitmapIndexZ = null;
		DynamicSequence predCount = null;

		global.reset();

		try {
			try {
				ExceptionIterator<Pair, IOException> sortedPairs;
				try (IteratorChunkedSource<Pair> chunkSource = IteratorChunkedSource.of(
						new ObjectAdjReader(seqZ, seqY, bitmapZ), p -> 3L * Long.BYTES, Math.max(1, chunkSize), null)) {
					sortedPairs = sort.sortPull(workers, chunkSource);
				}

				log.info("Pair sorted in {}", global.stopAndShow());

				global.reset();
				indexZ = createSequence64(diskLocation, "indexZ", BitUtil.log2(seqY.getNumberOfElements()),
						seqZ.getNumberOfElements());
				bitmapIndexZ = createBitmap375(diskLocation, "bitmapIndexZ", seqZ.getNumberOfElements());
				try {
					long lastObj = -2;
					long index = 0;

					long size = Math.max(seqZ.getNumberOfElements(), 1);
					long block = size < 10 ? 1 : size / 10;

					while (sortedPairs.hasNext()) {
						Pair pair = sortedPairs.next();

						long object = pair.object;
						long y = pair.predicatePosition;

						if (lastObj == object - 1) {
							// fill the bitmap index to denote the new object
							bitmapIndexZ.set(index - 1, true);
						} else if (!(lastObj == object)) {
							// non increasing Z?
							if (lastObj == -2) {
								if (object != 1) {
									throw new IllegalArgumentException("Pair object start after 1! " + object);
								}
								// start, ignore
							} else {
								throw new IllegalArgumentException(
										"Non 1 increasing object! lastObj: " + lastObj + ", object: " + object);
							}
						}
						lastObj = object;

						// fill the sequence with the predicate id
						if (index % block == 0) {
							listener.notifyProgress(index / (block / 10f),
									"writing bitmapIndexZ/indexZ " + index + "/" + size);
						}
						indexZ.set(index, y);
						index++;
					}
					listener.notifyProgress(100, "indexZ completed " + index);
					bitmapIndexZ.set(index - 1, true);
				} finally {
					IOUtil.closeObject(sortedPairs);
				}

				log.info("indexZ/bitmapIndexZ completed in {}", global.stopAndShow());
			} catch (KWayMerger.KWayMergerException | InterruptedException e) {
				if (e.getCause() != null) {
					IOUtil.throwIOOrRuntime(e.getCause());
				}
				throw new RuntimeException("Can't sort pairs", e);
			}

			global.reset();
			predCount = createSequence64(diskLocation, "predCount", BitUtil.log2(seqY.getNumberOfElements()),
					dictionary.getNpredicates());

			long size = Math.max(seqY.getNumberOfElements(), 1);
			long block = size < 10 ? 1 : size / 10;

			for (long i = 0; i < seqY.getNumberOfElements(); i++) {
				// Read value
				long val = seqY.get(i);

				if (i % block == 0) {
					listener.notifyProgress(i / (block / 10f), "writing predCount " + i + "/" + size);
				}
				// Increment
				predCount.set(val - 1, predCount.get(val - 1) + 1);
			}
			predCount.trimToSize();
			listener.notifyProgress(100, "predCount completed " + seqY.getNumberOfElements());
			log.info("Predicate count completed in {}", global.stopAndShow());
			IOUtil.closeObject(this.bitmapIndexZ);
		} catch (Throwable t) {
			try {
				throw t;
			} finally {
				Closer.closeAll(indexZ, bitmapIndexZ, predCount);
			}
		}
		this.predicateCount = predCount;
		this.indexZ = indexZ;
		this.bitmapIndexZ = bitmapIndexZ;
		this.adjIndex = new AdjacencyList(this.indexZ, this.bitmapIndexZ);
		log.info("Index generated in {}", global.stopAndShow());
	}

	private void createIndexObjectMemoryEfficient(ProgressListener listener) throws IOException {
		Path diskLocation;
		if (diskSequence) {
			diskLocation = diskSequenceLocation.createOrGetPath();
		} else {
			diskLocation = null;
		}

		StopWatch global = new StopWatch();
		StopWatch st = new StopWatch();
		ProgressListener progressListener = ProgressListener.ofNullable(listener);
		long totalEntries = seqZ.getNumberOfElements();

		// Count the number of appearances of each object
		long maxCount = 0;
		long numDifferentObjects = 0;
		long numReservedObjects = 8192;
		ModifiableBitmap bitmapIndex = null;
		DynamicSequence objectStart = null;
		DynamicSequence predCount = null;
		DynamicSequence objectArray = null;

		try {
			objectStart = createSequence64(diskLocation, "objectCount", BitUtil.log2(totalEntries), numReservedObjects);
			int countBucketSize = BucketedSequenceWriter.resolveObjectIndexBucketSize(totalEntries);
			int countBufferRecords = BucketedSequenceWriter.resolveObjectIndexBufferRecords(totalEntries,
					countBucketSize);
			try (BucketedSequenceWriter countWriter = BucketedSequenceWriter.create(diskLocation,
					"bitmapTriples-objectCountBuckets", totalEntries, countBucketSize, countBufferRecords)) {
				StageProgress countProgress = new StageProgress(progressListener.sub(0f, 10f), "count objects",
						totalEntries);

				for (long i = 0; i < totalEntries; i++) {
					long val = seqZ.get(i);
					if (val == 0) {
						throw new RuntimeException("ERROR: There is a zero value in the Z level.");
					}
					if (numReservedObjects < val) {
						while (numReservedObjects < val) {
							numReservedObjects <<= 1;
						}
					}
					if (numDifferentObjects < val) {
						numDifferentObjects = val;
					}

					countWriter.add(val - 1, 1L);
					countProgress.report(i + 1, false);

				}

				countProgress.finish(totalEntries);
				if (numReservedObjects < numDifferentObjects) {
					while (numReservedObjects < numDifferentObjects) {
						numReservedObjects <<= 1;
					}
				}
				objectStart.resize(numReservedObjects);

				ProgressListener materializeListener = new StageProgressListener(progressListener.sub(10f, 20f),
						"materialize object counts", numDifferentObjects);
				maxCount = countWriter.materializeCountsTo(objectStart, numDifferentObjects, materializeListener);
			}
			log.info("Count Objects in {} Max was: {}", st.stopAndShow(), maxCount);
			st.reset();

			// Calculate bitmap that separates each object sublist and prefix
			// starts.
			bitmapIndex = createBitmap375(diskLocation, "bitmapIndex", totalEntries);
			long tmpCount = 0;
			StageProgress bitmapProgress = new StageProgress(progressListener.sub(20f, 30f), "build bitmap",
					numDifferentObjects);
			for (long i = 0; i < numDifferentObjects; i++) {
				long count = objectStart.get(i);
				if (count != 0) {
					bitmapIndex.set(tmpCount + count - 1, true);
				}
				objectStart.set(i, tmpCount);
				tmpCount += count;
				bitmapProgress.report(i + 1, false);
			}
			bitmapProgress.finish(numDifferentObjects);
			if (totalEntries > 0) {
				bitmapIndex.set(totalEntries - 1, true);
			}
			log.info("Bitmap in {}", st.stopAndShow());
			st.reset();

			objectArray = createSequence64(diskLocation, "objectArray", BitUtil.log2(seqY.getNumberOfElements()),
					totalEntries, true);
			objectArray.resize(totalEntries);

			// Copy each object reference to its position
			try (DynamicSequence objectInsertedCount = createSequence64(diskLocation, "objectInsertedCount",
					BitUtil.log2(maxCount), numDifferentObjects)) {
				objectInsertedCount.resize(numDifferentObjects);

				int bucketSize = BucketedSequenceWriter.resolveObjectIndexBucketSize(totalEntries);
				int bufferRecords = BucketedSequenceWriter.resolveObjectIndexBufferRecords(totalEntries, bucketSize);
				try (BucketedSequenceWriter writer = BucketedSequenceWriter.create(diskLocation,
						"bitmapTriples-objectIndexBuckets", totalEntries, bucketSize, bufferRecords)) {
					boolean bucketParallelWrites = objectIndexParallelism > 1;
					if (bucketParallelWrites) {
						writer.setWriteParallelism(objectIndexParallelism);
					}

					// 2. Setup Batching (128k)
					final int BATCH_SIZE = 128 * 1024;
					boolean usePipeline = objectIndexParallelism > 1;
					boolean useParallelFill = objectIndexParallelism > 1;
					boolean useBatchPipeline = objectIndexParallelism > 1;
					objectIndexBucketParallelWritesEnabled = bucketParallelWrites;
					objectIndexPipelineEnabled = usePipeline;
					objectIndexBatchParallelFillEnabled = useParallelFill;
					objectIndexBatchProcessingPipelineEnabled = useBatchPipeline;
					int pipelineDepth = Math.min(4, Math.max(2, objectIndexParallelism));
					ObjectIndexPipeline pipeline = usePipeline
							? new ObjectIndexPipeline(writer, BATCH_SIZE, pipelineDepth)
							: null;
					ObjectIndexBatch directBatch = usePipeline ? null : new ObjectIndexBatch(BATCH_SIZE);
					ObjectIndexBatchScratch batchScratch = useBatchPipeline ? null
							: new ObjectIndexBatchScratch(BATCH_SIZE);
					ForkJoinPool fillPool = useBatchPipeline ? null
							: useParallelFill ? new ForkJoinPool(objectIndexParallelism) : null;
					ObjectIndexBatchPipeline batchPipeline = useBatchPipeline
							? new ObjectIndexBatchPipeline(this, BATCH_SIZE, pipelineDepth, objectIndexParallelism,
									objectStart, objectInsertedCount, writer, pipeline, directBatch)
							: null;
					ObjectIndexEntryBatch entryBatch = null;
					ObjectIndexEntry[] batch;
					if (batchPipeline != null) {
						entryBatch = batchPipeline.acquire();
						batch = entryBatch.entries;
					} else {
						batch = new ObjectIndexEntry[BATCH_SIZE];
						// Pre-allocate objects to avoid garbage collection
						// churn
						// in the loop
						for (int k = 0; k < BATCH_SIZE; k++) {
							batch[k] = new ObjectIndexEntry();
						}
					}
					int batchPtr = 0;

					StageProgress fillProgress = new StageProgress(progressListener.sub(30f, 65f),
							"fill object index buckets", totalEntries);
					try {
						// 3. Main Loop
						for (long i = 0; i < totalEntries; i++) {
							long objectValue = seqZ.get(i);

							// Vital: Calculate posY NOW while 'i' is
							// sequential.
							// This keeps bitmapZ.rank1 fast.
							long posY = i > 0 ? bitmapZ.rank1(i - 1) : 0;

							// Fill batch
							ObjectIndexEntry entry = batch[batchPtr++];
							entry.objectValue = objectValue;
							entry.posY = posY;

							// 4. Process Batch if full
							if (batchPtr == BATCH_SIZE) {
								if (batchPipeline != null) {
									entryBatch.length = batchPtr;
									batchPipeline.submit(entryBatch);
									entryBatch = batchPipeline.acquire();
									batch = entryBatch.entries;
								} else {
									writeObjectIndexBatch(batch, batchPtr, objectStart, objectInsertedCount, writer,
											pipeline, directBatch, batchScratch, fillPool);
								}
								batchPtr = 0;
							}
							fillProgress.report(i + 1, false);
						}

						// 5. Process Remaining Batch
						if (batchPtr > 0) {
							if (batchPipeline != null) {
								entryBatch.length = batchPtr;
								batchPipeline.submit(entryBatch);
							} else {
								writeObjectIndexBatch(batch, batchPtr, objectStart, objectInsertedCount, writer,
										pipeline, directBatch, batchScratch, fillPool);
							}
						}
						fillProgress.finish(totalEntries);
					} finally {
						if (batchPipeline != null) {
							batchPipeline.close();
						}
						if (pipeline != null) {
							pipeline.close();
						}
						if (fillPool != null) {
							fillPool.shutdown();
						}
					}

					ProgressListener materializeListener = new StageProgressListener(progressListener.sub(65f, 75f),
							"materialize buckets", totalEntries);
					writer.materializeTo(objectArray, materializeListener);
				}
				log.info("Object references in {}", st.stopAndShow());
			}
			IOUtil.closeObject(objectStart);
			objectStart = null;
			st.reset();

			int parallelism = objectIndexParallelism;
			if (numDifferentObjects > 0 && numDifferentObjects < parallelism) {
				parallelism = (int) numDifferentObjects;
			}
			StageProgress sortProgress = new StageProgress(progressListener.sub(75f, 90f), "sort object sublists",
					totalEntries);
			sortObjectSublists(seqY, objectArray, bitmapIndex, numDifferentObjects, parallelism, sortProgress);
			sortProgress.finish(totalEntries);

			log.info("Sort object sublists in {}", st.stopAndShow());
			st.reset();

			// Count predicates
			predCount = createSequence64(diskLocation, "predCount", BitUtil.log2(seqY.getNumberOfElements()), 0);
			long totalPredicates = seqY.getNumberOfElements();
			StageProgress predProgress = new StageProgress(progressListener.sub(90f, 100f), "count predicates",
					totalPredicates);
			for (long i = 0; i < totalPredicates; i++) {
				// Read value
				long val = seqY.get(i);

				// Grow if necessary
				if (predCount.getNumberOfElements() < val) {
					predCount.resize(val);
				}

				// Increment
				predCount.set(val - 1, predCount.get(val - 1) + 1);
				predProgress.report(i + 1, false);
			}
			predProgress.finish(totalPredicates);
			predCount.trimToSize();
			log.info("Count predicates in {}", st.stopAndShow());
		} catch (Throwable t) {
			try {
				throw t;
			} finally {
				try {
					IOUtil.closeObject(bitmapIndex);
				} finally {
					try {
						if (objectArray != null) {
							objectArray.close();
						}
					} finally {
						try {
							if (predCount != null) {
								predCount.close();
							}
						} finally {
							if (objectStart != null) {
								objectStart.close();
							}
						}
					}
				}
			}
		}

		this.predicateCount = predCount;
		st.reset();

		// Save Object Index
		this.indexZ = objectArray;
		this.bitmapIndexZ = bitmapIndex;
		this.adjIndex = new AdjacencyList(this.indexZ, this.bitmapIndexZ);

		log.info("Index generated in {}", global.stopAndShow());

	}

	private void writeObjectIndexBatch(ObjectIndexEntry[] batch, int batchPtr, DynamicSequence objectStart,
			DynamicSequence objectInsertedCount, BucketedSequenceWriter writer, ObjectIndexPipeline pipeline,
			ObjectIndexBatch directBatch, ObjectIndexBatchScratch scratch, ForkJoinPool fillPool) {
		Arrays.parallelSort(batch, 0, batchPtr, Comparator.comparingLong(o -> o.objectValue));

		ObjectIndexBatch target = pipeline != null ? pipeline.acquire() : directBatch;
		target.length = batchPtr;

		long[] groupObjects = scratch.groupObjects;
		int[] groupStarts = scratch.groupStarts;
		int[] groupLengths = scratch.groupLengths;
		long[] groupBases = scratch.groupBases;
		long[] groupOffsets = scratch.groupOffsets;

		int groupCount = 0;
		long currentObject = -1L;
		int groupStart = 0;
		for (int k = 0; k < batchPtr; k++) {
			long objectValue = batch[k].objectValue;
			if (objectValue != currentObject) {
				if (currentObject != -1L) {
					groupObjects[groupCount] = currentObject;
					groupStarts[groupCount] = groupStart;
					groupLengths[groupCount] = k - groupStart;
					groupCount++;
				}
				currentObject = objectValue;
				groupStart = k;
			}
		}
		if (currentObject != -1L) {
			groupObjects[groupCount] = currentObject;
			groupStarts[groupCount] = groupStart;
			groupLengths[groupCount] = batchPtr - groupStart;
			groupCount++;
		}

		for (int g = 0; g < groupCount; g++) {
			long objectValue = groupObjects[g];
			groupBases[g] = objectStart.get(objectValue - 1);
			groupOffsets[g] = objectInsertedCount.get(objectValue - 1);
		}

		long[] indexes = target.indexes;
		long[] values = target.values;
		int groupCountFinal = groupCount;
		boolean parallelFill = fillPool != null && shouldParallelFill(batchPtr, groupCountFinal);
		if (parallelFill) {
			try {
				fillPool.submit(() -> IntStream.range(0, groupCountFinal).parallel().forEach(g -> {
					long base = groupBases[g];
					long offset = groupOffsets[g];
					int start = groupStarts[g];
					int length = groupLengths[g];
					for (int i = 0; i < length; i++) {
						int idx = start + i;
						indexes[idx] = base + offset + i;
						values[idx] = batch[idx].posY;
					}
				})).get();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				throw new RuntimeException(e.getCause());
			}
		} else {
			for (int g = 0; g < groupCount; g++) {
				long base = groupBases[g];
				long offset = groupOffsets[g];
				int start = groupStarts[g];
				int length = groupLengths[g];
				for (int i = 0; i < length; i++) {
					int idx = start + i;
					indexes[idx] = base + offset + i;
					values[idx] = batch[idx].posY;
				}
			}
		}

		for (int g = 0; g < groupCount; g++) {
			long objectValue = groupObjects[g];
			objectInsertedCount.set(objectValue - 1, groupOffsets[g] + groupLengths[g]);
		}

		if (pipeline != null) {
			pipeline.submit(target);
		} else {
			writer.addBatch(target.indexes, target.values, target.length);
		}
	}

	private boolean shouldParallelFill(int batchSize, int groupCount) {
		if (objectIndexParallelism <= 1) {
			return false;
		}
		if (batchSize < OBJECT_INDEX_PARALLEL_FILL_MIN_RECORDS) {
			return false;
		}
		int minGroups = Math.max(OBJECT_INDEX_PARALLEL_FILL_MIN_GROUPS, objectIndexParallelism);
		return groupCount >= minGroups;
	}

	private void sortObjectSublists(Sequence seqY, DynamicSequence objectArray, ModifiableBitmap bitmapIndex,
			long numDifferentObjects, int parallelism, StageProgress progress) {
		if (numDifferentObjects <= 1) {
			return;
		}
		if (parallelism <= 1) {
			sortObjectSublistsSequential(seqY, objectArray, bitmapIndex, numDifferentObjects, progress);
			return;
		}
		sortObjectSublistsParallel(seqY, objectArray, bitmapIndex, numDifferentObjects, parallelism, progress);
	}

	private void sortObjectSublistsSequential(Sequence seqY, DynamicSequence objectArray, ModifiableBitmap bitmapIndex,
			long numDifferentObjects, StageProgress progress) {
		long object = 1;
		long first = 0;
		long last = bitmapIndex.selectNext1(first) + 1;
		long processed = 0;
		do {
			sortObjectRangeSequential(seqY, objectArray, first, last);
			long listLen = last - first;
			if (listLen > 0) {
				processed += listLen;
				if (progress != null) {
					progress.report(processed, false);
				}
			}
			first = last;
			last = bitmapIndex.selectNext1(first) + 1;
			object++;
		} while (object <= numDifferentObjects);
	}

	private void sortObjectSublistsParallel(Sequence seqY, DynamicSequence objectArray, ModifiableBitmap bitmapIndex,
			long numDifferentObjects, int parallelism, StageProgress progress) {
		long batchLimit = resolveObjectIndexSortBatchEntries(objectArray.getNumberOfElements(), parallelism);
		ForkJoinPool pool = new ForkJoinPool(parallelism);
		try {
			List<ObjectSortRange> ranges = new ArrayList<>();
			long batchEntries = 0L;
			long processed = 0L;

			long object = 1;
			long first = 0;
			long last = bitmapIndex.selectNext1(first) + 1;
			do {
				long listLen = last - first;
				if (listLen > 2) {
					if (listLen > Integer.MAX_VALUE) {
						throw new IllegalArgumentException("Object list too large to sort: " + listLen);
					}
					ranges.add(new ObjectSortRange(first, (int) listLen));
					batchEntries += listLen;
					if (batchEntries >= batchLimit) {
						sortObjectRangesBatch(pool, ranges, seqY, objectArray);
						ranges.clear();
						processed += batchEntries;
						if (progress != null) {
							progress.report(processed, false);
						}
						batchEntries = 0L;
					}
				} else if (listLen > 1) {
					sortObjectRangeSequential(seqY, objectArray, first, last);
					processed += listLen;
					if (progress != null) {
						progress.report(processed, false);
					}
				} else if (listLen > 0) {
					processed += listLen;
					if (progress != null) {
						progress.report(processed, false);
					}
				}
				first = last;
				last = bitmapIndex.selectNext1(first) + 1;
				object++;
			} while (object <= numDifferentObjects);

			if (!ranges.isEmpty()) {
				sortObjectRangesBatch(pool, ranges, seqY, objectArray);
				processed += batchEntries;
				if (progress != null) {
					progress.report(processed, false);
				}
			}
		} finally {
			pool.shutdown();
		}
	}

	private void sortObjectRangesBatch(ForkJoinPool pool, List<ObjectSortRange> ranges, Sequence seqY,
			DynamicSequence objectArray) {
		int rangeCount = ranges.size();
		long[][] sortedPositions = new long[rangeCount][];
		try {
			pool.submit(() -> IntStream.range(0, rangeCount).parallel().forEach(index -> {
				ObjectSortRange range = ranges.get(index);
				sortedPositions[index] = buildSortedPositions(seqY, objectArray, range.start, range.length);
			})).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e.getCause());
		}

		for (int i = 0; i < rangeCount; i++) {
			long[] positions = sortedPositions[i];
			if (positions == null) {
				continue;
			}
			ObjectSortRange range = ranges.get(i);
			long start = range.start;
			for (int j = 0; j < range.length; j++) {
				objectArray.set(start + j, positions[j]);
			}
			LongArrayPool.release(positions);
		}
	}

	private void sortObjectRangeSequential(Sequence seqY, DynamicSequence objectArray, long first, long last) {
		long listLen = last - first;
		if (listLen <= 1) {
			return;
		}
		if (listLen > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Object list too large to sort: " + listLen);
		}
		int length = (int) listLen;
		if (length == 2) {
			long aPos = objectArray.get(first);
			long bPos = objectArray.get(first + 1);
			long a = seqY.get(aPos);
			long b = seqY.get(bPos);
			if (a > b) {
				objectArray.set(first, bPos);
				objectArray.set(first + 1, aPos);
			}
			return;
		}

		long[] positions = buildSortedPositions(seqY, objectArray, first, length);
		try {
			for (int i = 0; i < length; i++) {
				objectArray.set(first + i, positions[i]);
			}
		} finally {
			LongArrayPool.release(positions);
		}
	}

	private static long[] buildSortedPositions(Sequence seqY, DynamicSequence objectArray, long start, int length) {
		long[] positions = LongArrayPool.borrow(length);
		long[] values = LongArrayPool.borrow(length);
		boolean releasePositions = true;
		try {
			for (int i = 0; i < length; i++) {
				long pos = objectArray.get(start + i);
				positions[i] = pos;
				values[i] = seqY.get(pos);
			}
			sortByValueThenPosition(values, positions, 0, length);
			releasePositions = false;
			return positions;
		} finally {
			LongArrayPool.release(values);
			if (releasePositions) {
				LongArrayPool.release(positions);
			}
		}
	}

	private static void sortByValueThenPosition(long[] values, long[] positions, int from, int to) {
		int length = to - from;
		if (length <= 1) {
			return;
		}
		int active = ACTIVE_TIMSORTS.incrementAndGet();
		if (shouldUseInPlaceSort(length, active)) {
			ACTIVE_TIMSORTS.decrementAndGet();
			if (shouldUseParallelInPlaceSort(length)) {
				parallelQuickSortPairs(values, positions, from, to - 1);
			} else {
				quickSortPairs(values, positions, from, to - 1);
			}
			return;
		}
		try {
			LongPairTimSort.sort(values, positions, from, to);
		} finally {
			ACTIVE_TIMSORTS.decrementAndGet();
		}
	}

	private static int comparePair(long valueA, long positionA, long valueB, long positionB) {
		int cmp = Long.compare(valueA, valueB);
		if (cmp != 0) {
			return cmp;
		}
		return Long.compare(positionA, positionB);
	}

	private static final int INSERTION_SORT_THRESHOLD = 32;
	private static final int PARALLEL_QUICKSORT_THRESHOLD = 1 << 15;
	private static final long TIMSORT_TMP_BYTES_PER_ENTRY = 8L;
	private static final long TIMSORT_TMP_SAFETY_BYTES = 8L * 1024L * 1024L;
	private static final int MEMORY_ESTIMATE_REFRESH_CALLS = 100;
	private static int MEMORY_ESTIMATE_CALLS = 0;
	private static volatile long MEMORY_ESTIMATE_CACHE = -1L;
	private static final AtomicInteger ACTIVE_TIMSORTS = new AtomicInteger();
	private static final Runtime runtime = Runtime.getRuntime();

	private static boolean shouldUseInPlaceSort(int length, int concurrentSorts) {
		long required = TIMSORT_TMP_BYTES_PER_ENTRY * (long) length;
		int sorts = Math.max(1, concurrentSorts);
		long perSortBudget = estimateAvailableMemory() / sorts;
		return required + TIMSORT_TMP_SAFETY_BYTES >= perSortBudget;
	}

	private static long estimateAvailableMemory() {
		long cached = MEMORY_ESTIMATE_CACHE;
		int calls = ++MEMORY_ESTIMATE_CALLS;
		if (cached >= 0 && (calls % MEMORY_ESTIMATE_REFRESH_CALLS) != 0) {
			return cached;
		}
		long used = runtime.totalMemory() - runtime.freeMemory();
		long available = runtime.maxMemory() - used;
		long clamped = Math.max(0L, available);
		MEMORY_ESTIMATE_CACHE = clamped;
		return clamped;
	}

	private static boolean shouldUseParallelInPlaceSort(int length) {
		if (length < PARALLEL_QUICKSORT_THRESHOLD) {
			return false;
		}
		return ForkJoinPool.getCommonPoolParallelism() > 1;
	}

	private static void parallelQuickSortPairs(long[] values, long[] positions, int left, int right) {
		if (left >= right) {
			return;
		}
		if (right - left < PARALLEL_QUICKSORT_THRESHOLD || ForkJoinPool.getCommonPoolParallelism() <= 1) {
			quickSortPairs(values, positions, left, right);
			return;
		}
		ForkJoinPool pool = ForkJoinTask.inForkJoinPool() ? ForkJoinTask.getPool() : ForkJoinPool.commonPool();
		if (pool == null) {
			quickSortPairs(values, positions, left, right);
			return;
		}
		pool.invoke(new LongPairParallelQuickSortTask(values, positions, left, right));
	}

	private static final class LongPairParallelQuickSortTask extends RecursiveAction {
		private final long[] values;
		private final long[] positions;
		private final int left;
		private final int right;

		private LongPairParallelQuickSortTask(long[] values, long[] positions, int left, int right) {
			this.values = values;
			this.positions = positions;
			this.left = left;
			this.right = right;
		}

		@Override
		protected void compute() {
			if (right - left < PARALLEL_QUICKSORT_THRESHOLD) {
				quickSortPairs(values, positions, left, right);
				return;
			}

			int mid = left + ((right - left) >>> 1);
			int pivotIndex = medianOfThree(values, positions, left, mid, right);
			long pivotValue = values[pivotIndex];
			long pivotPos = positions[pivotIndex];

			int i = left;
			int j = right;
			while (i <= j) {
				while (comparePair(values[i], positions[i], pivotValue, pivotPos) < 0) {
					i++;
				}
				while (comparePair(values[j], positions[j], pivotValue, pivotPos) > 0) {
					j--;
				}
				if (i <= j) {
					swapPairs(values, positions, i, j);
					i++;
					j--;
				}
			}

			LongPairParallelQuickSortTask leftTask = left < j
					? new LongPairParallelQuickSortTask(values, positions, left, j)
					: null;
			LongPairParallelQuickSortTask rightTask = i < right
					? new LongPairParallelQuickSortTask(values, positions, i, right)
					: null;

			if (leftTask != null && rightTask != null) {
				invokeAll(leftTask, rightTask);
			} else if (leftTask != null) {
				leftTask.compute();
			} else if (rightTask != null) {
				rightTask.compute();
			}
		}
	}

	private static void quickSortPairs(long[] values, long[] positions, int left, int right) {
		int lo = left;
		int hi = right;
		while (lo < hi) {
			if (hi - lo < INSERTION_SORT_THRESHOLD) {
				insertionSortPairs(values, positions, lo, hi);
				return;
			}
			int mid = lo + ((hi - lo) >>> 1);
			int pivotIndex = medianOfThree(values, positions, lo, mid, hi);
			long pivotValue = values[pivotIndex];
			long pivotPos = positions[pivotIndex];

			int i = lo;
			int j = hi;
			while (i <= j) {
				while (comparePair(values[i], positions[i], pivotValue, pivotPos) < 0) {
					i++;
				}
				while (comparePair(values[j], positions[j], pivotValue, pivotPos) > 0) {
					j--;
				}
				if (i <= j) {
					swapPairs(values, positions, i, j);
					i++;
					j--;
				}
			}

			if (j - lo < hi - i) {
				if (lo < j) {
					quickSortPairs(values, positions, lo, j);
				}
				lo = i;
			} else {
				if (i < hi) {
					quickSortPairs(values, positions, i, hi);
				}
				hi = j;
			}
		}
	}

	private static int medianOfThree(long[] values, long[] positions, int a, int b, int c) {
		if (comparePair(values[a], positions[a], values[b], positions[b]) < 0) {
			if (comparePair(values[b], positions[b], values[c], positions[c]) < 0) {
				return b;
			}
			return comparePair(values[a], positions[a], values[c], positions[c]) < 0 ? c : a;
		}
		if (comparePair(values[a], positions[a], values[c], positions[c]) < 0) {
			return a;
		}
		return comparePair(values[b], positions[b], values[c], positions[c]) < 0 ? c : b;
	}

	private static void insertionSortPairs(long[] values, long[] positions, int left, int right) {
		for (int i = left + 1; i <= right; i++) {
			long pivotValue = values[i];
			long pivotPos = positions[i];
			int j = i - 1;
			while (j >= left && comparePair(values[j], positions[j], pivotValue, pivotPos) > 0) {
				values[j + 1] = values[j];
				positions[j + 1] = positions[j];
				j--;
			}
			values[j + 1] = pivotValue;
			positions[j + 1] = pivotPos;
		}
	}

	private static void swapPairs(long[] values, long[] positions, int i, int j) {
		if (i == j) {
			return;
		}
		long value = values[i];
		values[i] = values[j];
		values[j] = value;

		long position = positions[i];
		positions[i] = positions[j];
		positions[j] = position;
	}

	/**
	 * TimSort implementation for two parallel long[] arrays.
	 * <p>
	 * Sort key is (values[i], positions[i]) using
	 * {@link #comparePair(long, long, long, long)}. This avoids boxing (Object
	 * TimSort) and avoids recursive quicksort.
	 * </p>
	 */
	private static final class LongPairTimSort {
		private static final int MIN_MERGE = 32;
		private static final int MIN_GALLOP = 7;
		private static final int INITIAL_TMP_STORAGE_LENGTH = 256;

		private final long[] values;
		private final long[] positions;
		private final int sortLen;

		private long[] tmpValues;
		private long[] tmpPositions;

		private final int[] runBase;
		private final int[] runLen;
		private int stackSize = 0;
		private int minGallop = MIN_GALLOP;

		private LongPairTimSort(long[] values, long[] positions, int sortLen) {
			this.values = values;
			this.positions = positions;
			this.sortLen = sortLen;

			int tmpLen = sortLen < 2 * INITIAL_TMP_STORAGE_LENGTH ? (sortLen >>> 1) : INITIAL_TMP_STORAGE_LENGTH;
			if (tmpLen < 1) {
				tmpLen = 1;
			}
			this.tmpValues = LongArrayPool.borrow(tmpLen);
			this.tmpPositions = LongArrayPool.borrow(tmpLen);

			int stackLen;
			if (sortLen < 120) {
				stackLen = 5;
			} else if (sortLen < 1542) {
				stackLen = 10;
			} else if (sortLen < 119151) {
				stackLen = 24;
			} else {
				stackLen = 40;
			}
			this.runBase = new int[stackLen];
			this.runLen = new int[stackLen];
		}

		static void sort(long[] values, long[] positions, int from, int to) {
			int n = to - from;
			if (n < 2) {
				return;
			}

			// Small arrays: detect the first run and finish with binary
			// insertion sort.
			if (n < MIN_MERGE) {
				int initRunLen = countRunAndMakeAscending(values, positions, from, to);
				binaryInsertionSort(values, positions, from, to, from + initRunLen);
				return;
			}

			LongPairTimSort ts = new LongPairTimSort(values, positions, n);
			try {
				int minRun = minRunLength(n);

				int lo = from;
				int remaining = n;
				do {
					int runLen = countRunAndMakeAscending(values, positions, lo, to);

					// If run is short, extend to minRun using binary insertion
					// sort.
					if (runLen < minRun) {
						int force = remaining <= minRun ? remaining : minRun;
						binaryInsertionSort(values, positions, lo, lo + force, lo + runLen);
						runLen = force;
					}

					ts.pushRun(lo, runLen);
					ts.mergeCollapse();

					lo += runLen;
					remaining -= runLen;
				} while (remaining != 0);

				ts.mergeForceCollapse();
			} finally {
				ts.releaseTmp();
			}
		}

		private static int minRunLength(int n) {
			int r = 0;
			while (n >= MIN_MERGE) {
				r |= (n & 1);
				n >>= 1;
			}
			return n + r;
		}

		private static int countRunAndMakeAscending(long[] values, long[] positions, int lo, int hi) {
			int runHi = lo + 1;
			if (runHi == hi) {
				return 1;
			}

			// Decide if ascending or descending.
			if (comparePair(values[runHi], positions[runHi], values[lo], positions[lo]) < 0) {
				// Descending
				while (runHi < hi
						&& comparePair(values[runHi], positions[runHi], values[runHi - 1], positions[runHi - 1]) < 0) {
					runHi++;
				}
				reverseRange(values, positions, lo, runHi);
			} else {
				// Ascending
				while (runHi < hi
						&& comparePair(values[runHi], positions[runHi], values[runHi - 1], positions[runHi - 1]) >= 0) {
					runHi++;
				}
			}
			return runHi - lo;
		}

		private static void reverseRange(long[] values, long[] positions, int lo, int hi) {
			int i = lo;
			int j = hi - 1;
			while (i < j) {
				long tmpV = values[i];
				values[i] = values[j];
				values[j] = tmpV;

				long tmpP = positions[i];
				positions[i] = positions[j];
				positions[j] = tmpP;

				i++;
				j--;
			}
		}

		private static void binaryInsertionSort(long[] values, long[] positions, int lo, int hi, int start) {
			if (start == lo) {
				start++;
			}

			for (; start < hi; start++) {
				long pivotValue = values[start];
				long pivotPos = positions[start];

				int left = lo;
				int right = start;

				// Upper bound (stable): insert after any existing equals.
				while (left < right) {
					int mid = (left + right) >>> 1;
					if (comparePair(pivotValue, pivotPos, values[mid], positions[mid]) < 0) {
						right = mid;
					} else {
						left = mid + 1;
					}
				}

				int n = start - left;
				if (n > 0) {
					System.arraycopy(values, left, values, left + 1, n);
					System.arraycopy(positions, left, positions, left + 1, n);
				}
				values[left] = pivotValue;
				positions[left] = pivotPos;
			}
		}

		private static int gallopLeft(long pivotValue, long pivotPos, long[] values, long[] positions, int base,
				int len, int hint) {
			int lastOfs = 0;
			int ofs = 1;
			if (comparePair(pivotValue, pivotPos, values[base + hint], positions[base + hint]) > 0) {
				int maxOfs = len - hint;
				while (ofs < maxOfs && comparePair(pivotValue, pivotPos, values[base + hint + ofs],
						positions[base + hint + ofs]) > 0) {
					lastOfs = ofs;
					ofs = (ofs << 1) + 1;
					if (ofs <= 0) {
						ofs = maxOfs;
					}
				}
				if (ofs > maxOfs) {
					ofs = maxOfs;
				}
				lastOfs += hint;
				ofs += hint;
			} else {
				int maxOfs = hint + 1;
				while (ofs < maxOfs && comparePair(pivotValue, pivotPos, values[base + hint - ofs],
						positions[base + hint - ofs]) <= 0) {
					lastOfs = ofs;
					ofs = (ofs << 1) + 1;
					if (ofs <= 0) {
						ofs = maxOfs;
					}
				}
				if (ofs > maxOfs) {
					ofs = maxOfs;
				}
				int tmp = lastOfs;
				lastOfs = hint - ofs;
				ofs = hint - tmp;
			}

			lastOfs++;
			while (lastOfs < ofs) {
				int mid = lastOfs + ((ofs - lastOfs) >>> 1);
				if (comparePair(pivotValue, pivotPos, values[base + mid], positions[base + mid]) > 0) {
					lastOfs = mid + 1;
				} else {
					ofs = mid;
				}
			}
			return ofs;
		}

		private static int gallopRight(long pivotValue, long pivotPos, long[] values, long[] positions, int base,
				int len, int hint) {
			int ofs = 1;
			int lastOfs = 0;
			if (comparePair(pivotValue, pivotPos, values[base + hint], positions[base + hint]) < 0) {
				int maxOfs = hint + 1;
				while (ofs < maxOfs && comparePair(pivotValue, pivotPos, values[base + hint - ofs],
						positions[base + hint - ofs]) < 0) {
					lastOfs = ofs;
					ofs = (ofs << 1) + 1;
					if (ofs <= 0) {
						ofs = maxOfs;
					}
				}
				if (ofs > maxOfs) {
					ofs = maxOfs;
				}
				int tmp = lastOfs;
				lastOfs = hint - ofs;
				ofs = hint - tmp;
			} else {
				int maxOfs = len - hint;
				while (ofs < maxOfs && comparePair(pivotValue, pivotPos, values[base + hint + ofs],
						positions[base + hint + ofs]) >= 0) {
					lastOfs = ofs;
					ofs = (ofs << 1) + 1;
					if (ofs <= 0) {
						ofs = maxOfs;
					}
				}
				if (ofs > maxOfs) {
					ofs = maxOfs;
				}
				lastOfs += hint;
				ofs += hint;
			}

			lastOfs++;
			while (lastOfs < ofs) {
				int mid = lastOfs + ((ofs - lastOfs) >>> 1);
				if (comparePair(pivotValue, pivotPos, values[base + mid], positions[base + mid]) < 0) {
					ofs = mid;
				} else {
					lastOfs = mid + 1;
				}
			}
			return ofs;
		}

		private void pushRun(int base, int len) {
			runBase[stackSize] = base;
			runLen[stackSize] = len;
			stackSize++;
		}

		private void mergeCollapse() {
			while (stackSize > 1) {
				int n = stackSize - 2;
				if ((n >= 1 && runLen[n - 1] <= runLen[n] + runLen[n + 1])
						|| (n >= 2 && runLen[n - 2] <= runLen[n - 1] + runLen[n])) {
					if (runLen[n - 1] < runLen[n + 1]) {
						n--;
					}
					mergeAt(n);
				} else if (runLen[n] <= runLen[n + 1]) {
					mergeAt(n);
				} else {
					break;
				}
			}
		}

		private void mergeForceCollapse() {
			while (stackSize > 1) {
				int n = stackSize - 2;
				if (n > 0 && runLen[n - 1] < runLen[n + 1]) {
					n--;
				}
				mergeAt(n);
			}
		}

		private void mergeAt(int i) {
			int base1 = runBase[i];
			int len1 = runLen[i];
			int base2 = runBase[i + 1];
			int len2 = runLen[i + 1];

			runLen[i] = len1 + len2;
			if (i == stackSize - 3) {
				runBase[i + 1] = runBase[i + 2];
				runLen[i + 1] = runLen[i + 2];
			}
			stackSize--;

			// Already ordered?
			if (comparePair(values[base1 + len1 - 1], positions[base1 + len1 - 1], values[base2],
					positions[base2]) <= 0) {
				return;
			}

			int k = gallopRight(values[base2], positions[base2], values, positions, base1, len1, 0);
			base1 += k;
			len1 -= k;
			if (len1 == 0) {
				return;
			}

			len2 = gallopLeft(values[base1 + len1 - 1], positions[base1 + len1 - 1], values, positions, base2, len2,
					len2 - 1);
			if (len2 == 0) {
				return;
			}

			// Copy the smaller run into temp to reduce memory traffic.
			if (len1 <= len2) {
				mergeLo(base1, len1, base2, len2);
			} else {
				mergeHi(base1, len1, base2, len2);
			}
		}

		private void mergeLo(int base1, int len1, int base2, int len2) {
			ensureCapacity(len1);

			System.arraycopy(values, base1, tmpValues, 0, len1);
			System.arraycopy(positions, base1, tmpPositions, 0, len1);

			int cursor1 = 0;
			int cursor2 = base2;
			int dest = base1;

			values[dest] = values[cursor2];
			positions[dest] = positions[cursor2];
			dest++;
			cursor2++;
			if (--len2 == 0) {
				System.arraycopy(tmpValues, cursor1, values, dest, len1);
				System.arraycopy(tmpPositions, cursor1, positions, dest, len1);
				return;
			}
			if (len1 == 1) {
				System.arraycopy(values, cursor2, values, dest, len2);
				System.arraycopy(positions, cursor2, positions, dest, len2);
				values[dest + len2] = tmpValues[cursor1];
				positions[dest + len2] = tmpPositions[cursor1];
				return;
			}

			int minGallop = this.minGallop;
			while (true) {
				int count1 = 0;
				int count2 = 0;

				do {
					if (comparePair(values[cursor2], positions[cursor2], tmpValues[cursor1],
							tmpPositions[cursor1]) < 0) {
						values[dest] = values[cursor2];
						positions[dest] = positions[cursor2];
						dest++;
						cursor2++;
						count2++;
						count1 = 0;
						if (--len2 == 0) {
							break;
						}
					} else {
						values[dest] = tmpValues[cursor1];
						positions[dest] = tmpPositions[cursor1];
						dest++;
						cursor1++;
						count1++;
						count2 = 0;
						if (--len1 == 1) {
							break;
						}
					}
				} while ((count1 | count2) < minGallop);

				if (len1 <= 1 || len2 == 0) {
					break;
				}

				do {
					count1 = gallopRight(values[cursor2], positions[cursor2], tmpValues, tmpPositions, cursor1, len1,
							0);
					if (count1 != 0) {
						System.arraycopy(tmpValues, cursor1, values, dest, count1);
						System.arraycopy(tmpPositions, cursor1, positions, dest, count1);
						dest += count1;
						cursor1 += count1;
						len1 -= count1;
						if (len1 <= 1) {
							break;
						}
					}
					values[dest] = values[cursor2];
					positions[dest] = positions[cursor2];
					dest++;
					cursor2++;
					if (--len2 == 0) {
						break;
					}

					count2 = gallopLeft(tmpValues[cursor1], tmpPositions[cursor1], values, positions, cursor2, len2, 0);
					if (count2 != 0) {
						System.arraycopy(values, cursor2, values, dest, count2);
						System.arraycopy(positions, cursor2, positions, dest, count2);
						dest += count2;
						cursor2 += count2;
						len2 -= count2;
						if (len2 == 0) {
							break;
						}
					}
					values[dest] = tmpValues[cursor1];
					positions[dest] = tmpPositions[cursor1];
					dest++;
					cursor1++;
					if (--len1 == 1) {
						break;
					}
					minGallop--;
				} while (count1 >= MIN_GALLOP | count2 >= MIN_GALLOP);

				if (minGallop < 0) {
					minGallop = 0;
				}
				minGallop += 2;
				if (len1 <= 1 || len2 == 0) {
					break;
				}
			}
			this.minGallop = minGallop < 1 ? 1 : minGallop;

			if (len1 == 1) {
				System.arraycopy(values, cursor2, values, dest, len2);
				System.arraycopy(positions, cursor2, positions, dest, len2);
				values[dest + len2] = tmpValues[cursor1];
				positions[dest + len2] = tmpPositions[cursor1];
			} else if (len1 == 0) {
				throw new IllegalArgumentException("Comparison method violates its general contract!");
			} else {
				System.arraycopy(tmpValues, cursor1, values, dest, len1);
				System.arraycopy(tmpPositions, cursor1, positions, dest, len1);
			}
		}

		private void mergeHi(int base1, int len1, int base2, int len2) {
			ensureCapacity(len2);

			System.arraycopy(values, base2, tmpValues, 0, len2);
			System.arraycopy(positions, base2, tmpPositions, 0, len2);

			int cursor1 = base1 + len1 - 1;
			int cursor2 = len2 - 1;
			int dest = base2 + len2 - 1;

			values[dest] = values[cursor1];
			positions[dest] = positions[cursor1];
			dest--;
			cursor1--;
			if (--len1 == 0) {
				System.arraycopy(tmpValues, 0, values, dest - (len2 - 1), len2);
				System.arraycopy(tmpPositions, 0, positions, dest - (len2 - 1), len2);
				return;
			}
			if (len2 == 1) {
				dest -= len1;
				cursor1 -= len1;
				System.arraycopy(values, cursor1 + 1, values, dest + 1, len1);
				System.arraycopy(positions, cursor1 + 1, positions, dest + 1, len1);
				values[dest] = tmpValues[cursor2];
				positions[dest] = tmpPositions[cursor2];
				return;
			}

			int minGallop = this.minGallop;
			while (true) {
				int count1 = 0;
				int count2 = 0;

				do {
					if (comparePair(tmpValues[cursor2], tmpPositions[cursor2], values[cursor1],
							positions[cursor1]) < 0) {
						values[dest] = values[cursor1];
						positions[dest] = positions[cursor1];
						dest--;
						cursor1--;
						count1++;
						count2 = 0;
						if (--len1 == 0) {
							break;
						}
					} else {
						values[dest] = tmpValues[cursor2];
						positions[dest] = tmpPositions[cursor2];
						dest--;
						cursor2--;
						count2++;
						count1 = 0;
						if (--len2 == 1) {
							break;
						}
					}
				} while ((count1 | count2) < minGallop);

				if (len1 == 0 || len2 <= 1) {
					break;
				}

				do {
					count1 = len1 - gallopRight(tmpValues[cursor2], tmpPositions[cursor2], values, positions, base1,
							len1, len1 - 1);
					if (count1 != 0) {
						dest -= count1;
						cursor1 -= count1;
						len1 -= count1;
						System.arraycopy(values, cursor1 + 1, values, dest + 1, count1);
						System.arraycopy(positions, cursor1 + 1, positions, dest + 1, count1);
						if (len1 == 0) {
							break;
						}
					}
					values[dest] = tmpValues[cursor2];
					positions[dest] = tmpPositions[cursor2];
					dest--;
					cursor2--;
					if (--len2 == 1) {
						break;
					}

					count2 = len2 - gallopLeft(values[cursor1], positions[cursor1], tmpValues, tmpPositions, 0, len2,
							len2 - 1);
					if (count2 != 0) {
						dest -= count2;
						cursor2 -= count2;
						len2 -= count2;
						System.arraycopy(tmpValues, cursor2 + 1, values, dest + 1, count2);
						System.arraycopy(tmpPositions, cursor2 + 1, positions, dest + 1, count2);
						if (len2 <= 1) {
							break;
						}
					}
					values[dest] = values[cursor1];
					positions[dest] = positions[cursor1];
					dest--;
					cursor1--;
					if (--len1 == 0) {
						break;
					}
					minGallop--;
				} while (count1 >= MIN_GALLOP | count2 >= MIN_GALLOP);

				if (minGallop < 0) {
					minGallop = 0;
				}
				minGallop += 2;
				if (len1 == 0 || len2 <= 1) {
					break;
				}
			}
			this.minGallop = minGallop < 1 ? 1 : minGallop;

			if (len2 == 1) {
				dest -= len1;
				cursor1 -= len1;
				System.arraycopy(values, cursor1 + 1, values, dest + 1, len1);
				System.arraycopy(positions, cursor1 + 1, positions, dest + 1, len1);
				values[dest] = tmpValues[cursor2];
				positions[dest] = tmpPositions[cursor2];
			} else if (len2 == 0) {
				throw new IllegalArgumentException("Comparison method violates its general contract!");
			} else {
				System.arraycopy(tmpValues, 0, values, dest - (len2 - 1), len2);
				System.arraycopy(tmpPositions, 0, positions, dest - (len2 - 1), len2);
			}
		}

		private void ensureCapacity(int minCapacity) {
			if (tmpValues.length >= minCapacity && tmpPositions.length >= minCapacity) {
				return;
			}

			int maxTmp = sortLen >>> 1;
			if (maxTmp < 1) {
				maxTmp = 1;
			}

			int newSize = minCapacity;
			// Grow roughly to next power-of-two to reduce realloc churn.
			newSize |= (newSize >> 1);
			newSize |= (newSize >> 2);
			newSize |= (newSize >> 4);
			newSize |= (newSize >> 8);
			newSize |= (newSize >> 16);
			newSize++;

			if (newSize < 0) {
				newSize = minCapacity;
			} else if (newSize > maxTmp) {
				newSize = maxTmp;
			}

			if (newSize < minCapacity) {
				newSize = minCapacity;
			}

			long[] oldValues = tmpValues;
			long[] oldPositions = tmpPositions;
			tmpValues = LongArrayPool.borrow(newSize);
			tmpPositions = LongArrayPool.borrow(newSize);
			LongArrayPool.release(oldValues);
			LongArrayPool.release(oldPositions);
		}

		private void releaseTmp() {
			LongArrayPool.release(tmpValues);
			LongArrayPool.release(tmpPositions);
			tmpValues = new long[1];
			tmpPositions = new long[1];
		}
	}

	private static long resolveObjectIndexSortBatchEntries(long totalEntries, int parallelism) {
		long base = totalEntries / Math.max(1L, parallelism * 4L);
		long target = Math.max(100_000L, base);
		return Math.max(1L, Math.min(1_000_000L, target));
	}

	private static final class ObjectSortRange {
		private final long start;
		private final int length;

		private ObjectSortRange(long start, int length) {
			this.start = start;
			this.length = length;
		}
	}

	private static final class ObjectIndexEntry {
		private long objectValue;
		private long posY;
	}

	private static final class ObjectIndexEntryBatch {
		private final ObjectIndexEntry[] entries;
		private int length;

		private ObjectIndexEntryBatch(int capacity) {
			this.entries = new ObjectIndexEntry[capacity];
			for (int i = 0; i < capacity; i++) {
				entries[i] = new ObjectIndexEntry();
			}
		}
	}

	private static final class ObjectIndexBatch {
		private final long[] indexes;
		private final long[] values;
		private int length;

		private ObjectIndexBatch(int capacity) {
			this.indexes = new long[capacity];
			this.values = new long[capacity];
		}
	}

	private static final class ObjectIndexBatchScratch {
		private final long[] groupObjects;
		private final int[] groupStarts;
		private final int[] groupLengths;
		private final long[] groupBases;
		private final long[] groupOffsets;

		private ObjectIndexBatchScratch(int capacity) {
			groupObjects = new long[capacity];
			groupStarts = new int[capacity];
			groupLengths = new int[capacity];
			groupBases = new long[capacity];
			groupOffsets = new long[capacity];
		}
	}

	private static final class StageProgress {
		private static final ThreadLocal<NumberFormat> GROUPED_NUMBERS = ThreadLocal.withInitial(() -> {
			NumberFormat format = NumberFormat.getIntegerInstance(Locale.US);
			format.setGroupingUsed(true);
			format.setMaximumFractionDigits(0);
			format.setMinimumFractionDigits(0);
			return format;
		});
		private final ProgressListener listener;
		private final String stage;
		private final long total;
		private final long startMillis;
		private long lastReportMillis;

		private StageProgress(ProgressListener listener, String stage, long total) {
			this.listener = ProgressListener.ofNullable(listener);
			this.stage = stage;
			this.total = Math.max(0L, total);
			this.startMillis = System.currentTimeMillis();
			this.lastReportMillis = startMillis;
		}

		int sinceLastReport = 0;

		private void report(long processed, boolean force) {
			long clamped = Math.max(0L, Math.min(processed, total));

			if (sinceLastReport++ % REPORT_INTERVAL != 0 && !force) {
				return;
			}

			long now = System.currentTimeMillis();
			if (!force && (now - lastReportMillis) < OBJECT_INDEX_PROGRESS_MIN_INTERVAL_MILLIS) {
				return;
			}
			lastReportMillis = now;

			double elapsedSeconds = (now - startMillis) / 1_000d;
			long rate = elapsedSeconds > 0d ? (long) (clamped / elapsedSeconds) : clamped;
			float percent = total > 0L ? (float) (clamped * 100d / total) : 100f;
			listener.notifyProgress(percent, "object index: {} {}/{} ({} items/s)", stage, formatCount(clamped),
					formatCount(total), formatCount(rate));
		}

		private void finish(long processed) {
			report(processed, true);
		}

		private static String formatCount(long value) {
			return GROUPED_NUMBERS.get().format(value);
		}
	}

	private static final class StageProgressListener implements ProgressListener {
		private final StageProgress progress;
		private final long total;
		private long lastNotified = 0;

		private StageProgressListener(ProgressListener listener, String stage, long total) {
			this.total = Math.max(0L, total);
			this.progress = new StageProgress(listener, stage, this.total);
		}

		@Override
		public void notifyProgress(float level, String message) {
			float clampedLevel = Math.max(0f, Math.min(100f, level));
			long processed = total > 0L ? (long) Math.floor(total * (clampedLevel / 100f)) : 0L;
			progress.report(processed, ++lastNotified % 100 == 0);
		}
	}

	private static final class ObjectIndexBatchPipeline implements AutoCloseable {
		private static final long QUEUE_WAIT_TIMEOUT_MS = 250L;
		private final BitmapTriples owner;
		private final BlockingQueue<ObjectIndexEntryBatch> freeBatches;
		private final BlockingQueue<ObjectIndexEntryBatch> readyBatches;
		private final ObjectIndexEntryBatch poison;
		private final Thread worker;
		private final AtomicReference<Throwable> error;
		private final DynamicSequence objectStart;
		private final DynamicSequence objectInsertedCount;
		private final BucketedSequenceWriter writer;
		private final ObjectIndexPipeline pipeline;
		private final ObjectIndexBatch directBatch;
		private final ObjectIndexBatchScratch scratch;
		private final ForkJoinPool fillPool;

		private ObjectIndexBatchPipeline(BitmapTriples owner, int batchSize, int depth, int parallelism,
				DynamicSequence objectStart, DynamicSequence objectInsertedCount, BucketedSequenceWriter writer,
				ObjectIndexPipeline pipeline, ObjectIndexBatch directBatch) {
			this.owner = owner;
			this.objectStart = objectStart;
			this.objectInsertedCount = objectInsertedCount;
			this.writer = writer;
			this.pipeline = pipeline;
			this.directBatch = directBatch;
			int capacity = Math.max(1, depth);
			freeBatches = new ArrayBlockingQueue<>(capacity);
			readyBatches = new ArrayBlockingQueue<>(capacity);
			for (int i = 0; i < capacity; i++) {
				freeBatches.add(new ObjectIndexEntryBatch(batchSize));
			}
			poison = new ObjectIndexEntryBatch(0);
			error = new AtomicReference<>();
			scratch = new ObjectIndexBatchScratch(batchSize);
			fillPool = parallelism > 1 ? new ForkJoinPool(parallelism) : null;
			worker = new Thread(this::run, "BitmapTriples-object-index-batch");
			worker.setDaemon(true);
			worker.start();
		}

		private void run() {
			try {
				while (true) {
					ObjectIndexEntryBatch batch = take(readyBatches);
					if (batch == poison) {
						return;
					}
					owner.writeObjectIndexBatch(batch.entries, batch.length, objectStart, objectInsertedCount, writer,
							pipeline, directBatch, scratch, fillPool);
					batch.length = 0;
					put(freeBatches, batch);
				}
			} catch (Throwable t) {
				error.set(t);
			}
		}

		private ObjectIndexEntryBatch acquire() {
			checkError();
			return take(freeBatches);
		}

		private void submit(ObjectIndexEntryBatch batch) {
			checkError();
			put(readyBatches, batch);
		}

		@Override
		public void close() {
			put(readyBatches, poison);
			try {
				worker.join();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			if (fillPool != null) {
				fillPool.shutdown();
			}
			checkError();
		}

		private void checkError() {
			Throwable t = error.get();
			if (t != null) {
				throw new RuntimeException(t);
			}
		}

		private static ObjectIndexEntryBatch take(BlockingQueue<ObjectIndexEntryBatch> queue) {
			while (true) {
				try {
					ObjectIndexEntryBatch batch = queue.poll(QUEUE_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
					if (batch != null) {
						return batch;
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(e);
				}
			}
		}

		private static void put(BlockingQueue<ObjectIndexEntryBatch> queue, ObjectIndexEntryBatch batch) {
			while (true) {
				try {
					if (queue.offer(batch, QUEUE_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
						return;
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(e);
				}
			}
		}
	}

	private static final class ObjectIndexPipeline implements AutoCloseable {
		private static final long QUEUE_WAIT_TIMEOUT_MS = 250L;
		private final BlockingQueue<ObjectIndexBatch> freeBatches;
		private final BlockingQueue<ObjectIndexBatch> readyBatches;
		private final ObjectIndexBatch poison;
		private final Thread worker;
		private final AtomicReference<Throwable> error;
		private final BucketedSequenceWriter writer;

		private ObjectIndexPipeline(BucketedSequenceWriter writer, int batchSize, int depth) {
			this.writer = writer;
			int capacity = Math.max(1, depth);
			freeBatches = new ArrayBlockingQueue<>(capacity);
			readyBatches = new ArrayBlockingQueue<>(capacity);
			for (int i = 0; i < capacity; i++) {
				freeBatches.add(new ObjectIndexBatch(batchSize));
			}
			poison = new ObjectIndexBatch(0);
			error = new AtomicReference<>();
			worker = new Thread(this::run, "BitmapTriples-object-index-writer");
			worker.setDaemon(true);
			worker.start();
		}

		private void run() {
			try {
				while (true) {
					ObjectIndexBatch batch = take(readyBatches);
					if (batch == poison) {
						return;
					}
					writer.addBatch(batch.indexes, batch.values, batch.length);
					batch.length = 0;
					put(freeBatches, batch);
				}
			} catch (Throwable t) {
				error.set(t);
			}
		}

		private ObjectIndexBatch acquire() {
			checkError();
			return take(freeBatches);
		}

		private void submit(ObjectIndexBatch batch) {
			checkError();
			put(readyBatches, batch);
		}

		@Override
		public void close() {
			put(readyBatches, poison);
			try {
				worker.join();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			checkError();
		}

		private void checkError() {
			Throwable t = error.get();
			if (t != null) {
				throw new RuntimeException(t);
			}
		}

		private static ObjectIndexBatch take(BlockingQueue<ObjectIndexBatch> queue) {
			while (true) {
				try {
					ObjectIndexBatch batch = queue.poll(QUEUE_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
					if (batch != null) {
						return batch;
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(e);
				}
			}
		}

		private static void put(BlockingQueue<ObjectIndexBatch> queue, ObjectIndexBatch batch) {
			while (true) {
				try {
					if (queue.offer(batch, QUEUE_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
						return;
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(e);
				}
			}
		}
	}

	private void createIndexObjects() {
		class Pair {
			int valueY;
			int positionY;
		}

		ArrayList<List<Pair>> list = new ArrayList<>();

		System.out.println("Generating HDT Index for ?PO, and ??O queries.");
		// Generate lists
		long total = seqZ.getNumberOfElements();
		for (long i = 0; i < total; i++) {
			Pair pair = new Pair();
			pair.positionY = (int) adjZ.findListIndex(i);
			pair.valueY = (int) seqY.get(pair.positionY);

			long valueZ = seqZ.get(i);

			if (list.size() <= (int) valueZ) {
				list.ensureCapacity((int) valueZ);
				while (list.size() < valueZ) {
					list.add(new ArrayList<>(1));
				}
			}

			List<Pair> inner = list.get((int) valueZ - 1);
			if (inner == null) {
				inner = new ArrayList<>(1);
				list.set((int) valueZ - 1, inner);
			}

			inner.add(pair);

		}

		System.out.println("Serialize object lists");
		// Serialize
		DynamicSequence indexZ = new SequenceLog64(BitUtil.log2(seqY.getNumberOfElements()), list.size());
		Bitmap375Big bitmapIndexZ = Bitmap375Big.memory(seqY.getNumberOfElements());
		long pos = 0;

		total = list.size();
		for (int i = 0; i < total; i++) {
			List<Pair> inner = list.get(i);

			// Sort by Y
			inner.sort(Comparator.comparingInt(o -> o.valueY));

			// Serialize
			for (int j = 0; j < inner.size(); j++) {
				indexZ.append(inner.get(j).positionY);

				bitmapIndexZ.set(pos, j == inner.size() - 1);
				pos++;
			}

			// Dereference processed list to let GC release the memory.
			list.set(i, null);
		}

		// Count predicates
		SequenceLog64 predCount = new SequenceLog64(BitUtil.log2(seqY.getNumberOfElements()));
		for (long i = 0; i < seqY.getNumberOfElements(); i++) {
			// Read value
			long val = seqY.get(i);

			// Grow if necessary
			if (predCount.getNumberOfElements() < val) {
				predCount.resize(val);
			}

			// Increment
			predCount.set(val - 1, predCount.get(val - 1) + 1);
		}
		predCount.trimToSize();
		this.predicateCount = predCount;

		this.indexZ = indexZ;
		this.bitmapIndexZ = bitmapIndexZ;
		this.adjIndex = new AdjacencyList(this.indexZ, this.bitmapIndexZ);
	}

	@Override
	public void generateIndex(ProgressListener listener, HDTOptions specIndex, Dictionary dictionary)
			throws IOException {
		loadDiskSequence(specIndex);
		objectIndexParallelism = resolveObjectIndexParallelism(specIndex);
		objectIndexPipelineEnabled = false;
		objectIndexBucketParallelWritesEnabled = false;
		objectIndexBatchParallelFillEnabled = false;
		objectIndexBatchProcessingPipelineEnabled = false;

		String indexMethod = specIndex.get(HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_KEY,
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_RECOMMENDED);
		switch (indexMethod) {
		case HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_RECOMMENDED,
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_OPTIMIZED ->
			createIndexObjectMemoryEfficient(listener);
		case HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK ->
			createIndexObjectDisk(specIndex, dictionary, listener);
		case HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_LEGACY -> createIndexObjects();
		default -> throw new IllegalArgumentException("Unknown INDEXING METHOD: " + indexMethod);
		}

		predicateIndex = new PredicateIndexArray(this);
		if (!specIndex.getBoolean("debug.bitmaptriples.ignorePredicateIndex", false)) {
			predicateIndex.generate(listener, specIndex, dictionary);
		} else {
			System.err.println("WARNING!!! PREDICATE INDEX IGNORED, THE INDEX WON'T BE COMPLETED!");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#populateHeader(hdt.header.Header,
	 * java.lang.String)
	 */
	@Override
	public void populateHeader(Header header, String rootNode) {
		if (rootNode == null || rootNode.length() == 0) {
			throw new IllegalArgumentException("Root node for the header cannot be null");
		}

		header.insert(rootNode, HDTVocabulary.TRIPLES_TYPE, getType());
		header.insert(rootNode, HDTVocabulary.TRIPLES_NUM_TRIPLES, getNumberOfElements());
		header.insert(rootNode, HDTVocabulary.TRIPLES_ORDER, order.toString());
//		header.insert(rootNode, HDTVocabulary.TRIPLES_SEQY_TYPE, seqY.getType() );
//		header.insert(rootNode, HDTVocabulary.TRIPLES_SEQZ_TYPE, seqZ.getType() );
//		header.insert(rootNode, HDTVocabulary.TRIPLES_SEQY_SIZE, seqY.size() );
//		header.insert(rootNode, HDTVocabulary.TRIPLES_SEQZ_SIZE, seqZ.size() );
//		if(bitmapY!=null) {
//			header.insert(rootNode, HDTVocabulary.TRIPLES_BITMAPY_SIZE, bitmapY.getSizeBytes() );
//		}
//		if(bitmapZ!=null) {
//			header.insert(rootNode, HDTVocabulary.TRIPLES_BITMAPZ_SIZE, bitmapZ.getSizeBytes() );
//		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#getType()
	 */
	@Override
	public String getType() {
		return HDTVocabulary.TRIPLES_TYPE_BITMAP;
	}

	@Override
	public TripleID findTriple(long position, TripleID tripleID) {
		if (position == 0) {
			// remove this special case so we can use position-1
			tripleID.setAll(1, seqY.get(0), seqZ.get(0));
			return tripleID;
		}
		// get the object at the given position
		long z = seqZ.get(position);

		// -1 so we don't count end of tree
		long posY = bitmapZ.rank1(position - 1);
		long y = seqY.get(posY);

		if (posY == 0) {
			// remove this case to do posY - 1
			tripleID.setAll(1, y, z);
			return tripleID;
		}

		// -1 so we don't count end of tree
		long posX = bitmapY.rank1(posY - 1);
		long x = posX + 1; // the subject ID is the position + 1, IDs start from
		// 1 not zero

		tripleID.setAll(x, y, z);
		return tripleID;
	}

	@Override
	public List<TripleComponentOrder> getTripleComponentOrder(TripleID pattern) {
		if (isClosed) {
			throw new IllegalStateException("Cannot search on BitmapTriples if it's already closed");
		}

		if (getNumberOfElements() == 0 || pattern.isNoMatch()) {
			return List.of(TripleComponentOrder.POS, TripleComponentOrder.PSO, TripleComponentOrder.SPO,
					TripleComponentOrder.SOP, TripleComponentOrder.OSP, TripleComponentOrder.OPS);
		}

		TripleID reorderedPat = new TripleID(pattern);
		TripleOrderConvert.swapComponentOrder(reorderedPat, TripleComponentOrder.SPO, order);
		int flags = reorderedPat.getPatternOrderFlags();

		if ((indexesMask & flags) != 0) {
			return TripleComponentOrder.fetchAllBestForCfg(flags, indexes);
		}

		return List.of();

	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#saveIndex(java.io.OutputStream,
	 * hdt.options.ControlInfo, hdt.listener.ProgressListener)
	 */
	@Override
	public void saveIndex(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		IntermediateListener iListener = new IntermediateListener(listener);

		ci.clear();
		ci.setType(ControlInfo.Type.INDEX);
		ci.setInt("numTriples", getNumberOfElements());
		ci.setInt("order", order.ordinal());
		ci.setFormat(HDTVocabulary.INDEX_TYPE_FOQ);
		ci.save(output);

		bitmapIndexZ.save(output, iListener);
		indexZ.save(output, iListener);

		predicateIndex.save(output);

		predicateCount.save(output, iListener);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#loadIndex(java.io.InputStream,
	 * hdt.options.ControlInfo, hdt.listener.ProgressListener)
	 */
	@Override
	public void loadIndex(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		IntermediateListener iListener = new IntermediateListener(listener);

		if (ci.getType() != ControlInfo.Type.INDEX) {
			throw new IllegalFormatException("Trying to read an Index Section but it was not an Index.");
		}

		if (!HDTVocabulary.INDEX_TYPE_FOQ.equals(ci.getFormat())) {
			throw new IllegalFormatException(
					"Trying to read wrong format of Index. Remove the .hdt.index file and let the app regenerate it.");
		}

		long numTriples = ci.getInt("numTriples");
		if (this.getNumberOfElements() != numTriples) {
			throw new IllegalFormatException("This index is not associated to the HDT file");
		}

		TripleComponentOrder indexOrder = TripleComponentOrder.values()[(int) ci.getInt("order")];
		if (indexOrder != order) {
			throw new IllegalFormatException("The order of the triples is not the same of the index.");
		}

		IOUtil.closeObject(bitmapIndexZ);
		bitmapIndexZ = null;

		bitmapIndexZ = BitmapFactory.createBitmap(input);
		try {
			bitmapIndexZ.load(input, iListener);

			if (indexZ != null) {
				try {
					indexZ.close();
				} finally {
					indexZ = null;
				}
			}

			indexZ = SequenceFactory.createStream(input);
			try {
				indexZ.load(input, iListener);

				if (predicateIndex != null) {
					try {
						predicateIndex.close();
					} finally {
						predicateIndex = null;
					}
				}

				predicateIndex = new PredicateIndexArray(this);
				try {
					predicateIndex.load(input);

					if (predicateCount != null) {
						try {
							predicateCount.close();
						} finally {
							predicateCount = null;
						}
					}

					predicateCount = SequenceFactory.createStream(input);
					try {
						predicateCount.load(input, iListener);

						this.adjIndex = new AdjacencyList(this.indexZ, this.bitmapIndexZ);
					} catch (Throwable t) {
						try {
							predicateCount.close();
						} finally {
							predicateCount = null;
							this.adjIndex = null;
						}
						throw t;
					}
				} catch (Throwable t) {
					try {
						predicateIndex.close();
					} finally {
						predicateIndex = null;
					}
					throw t;
				}
			} catch (Throwable t) {
				try {
					indexZ.close();
				} finally {
					indexZ = null;
				}
				throw t;
			}
		} catch (Throwable t) {
			try {
				IOUtil.closeObject(bitmapIndexZ);
			} finally {
				bitmapIndexZ = null;
			}
			throw t;
		}
	}

	@Override
	public void mapIndex(CountInputStream input, File f, ControlInfo ci, ProgressListener listener) throws IOException {
		IntermediateListener iListener = new IntermediateListener(listener);

		if (ci.getType() != ControlInfo.Type.INDEX) {
			throw new IllegalFormatException("Trying to read an Index Section but it was not an Index.");
		}

		if (!HDTVocabulary.INDEX_TYPE_FOQ.equals(ci.getFormat())) {
			throw new IllegalFormatException(
					"Trying to read wrong format of Index. Remove the .hdt.index file and let the app regenerate it.");
		}

		long numTriples = ci.getInt("numTriples");
		if (this.getNumberOfElements() != numTriples) {
			throw new IllegalFormatException("This index is not associated to the HDT file");
		}

		TripleComponentOrder indexOrder = TripleComponentOrder.values()[(int) ci.getInt("order")];
		if (indexOrder != order) {
			throw new IllegalFormatException("The order of the triples is not the same of the index.");
		}

		bitmapIndexZ = BitmapFactory.createBitmap(input);
		bitmapIndexZ.load(input, iListener);

		indexZ = SequenceFactory.createStream(input, f);

		if (predicateIndex != null) {
			predicateIndex.close();
		}

		predicateIndex = new PredicateIndexArray(this);
		predicateIndex.mapIndex(input, f, iListener);

		predicateCount = SequenceFactory.createStream(input, f);

		this.adjIndex = new AdjacencyList(this.indexZ, this.bitmapIndexZ);
	}

	@Override
	public void mapGenOtherIndexes(Path file, HDTOptions spec, ProgressListener listener) throws IOException {
		syncOtherIndexes(file, spec, listener);
	}

	@Override
	public void close() throws IOException {
		isClosed = true;
		try {
			Closer.closeAll(seqY, seqZ, indexZ, predicateCount, predicateIndex, bitmapIndexZ, diskSequenceLocation,
					indexes);
		} finally {
			diskSequenceLocation = null;
			seqY = null;
			seqZ = null;
			indexZ = null;
			predicateCount = null;
			predicateIndex = null;
			bitmapIndexZ = null;
			indexes.clear();
			indexesMask = 0;
		}
	}

	public boolean hasFOQIndex() {
		return indexZ != null && bitmapIndexZ != null;
	}

	public void syncOtherIndexes(Path fileLocation, HDTOptions spec, ProgressListener listener) throws IOException {
		Closer.closeAll(indexes);
		indexes.clear();
		indexesMask = 0;

		if (fileLocation == null) {
			return;
		}

		String otherIdxs = spec.get(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "");
		boolean allowOldOthers = spec.getBoolean(HDTOptionsKeys.BITMAPTRIPLES_INDEX_ALLOW_OLD_OTHERS, false);

		Set<TripleComponentOrder> askedOrders = Arrays.stream(otherIdxs.toUpperCase().split(",")).map(e -> {
			if (e.isEmpty() || e.equalsIgnoreCase(TripleComponentOrder.Unknown.name())) {
				return null;
			}
			try {
				return TripleComponentOrder.valueOf(e);
			} catch (IllegalArgumentException ex) {
				log.warn("Trying to use a bad order name {}", e, ex);
				return null;
			}
		}).filter(Objects::nonNull).collect(Collectors.toSet());

		MultiThreadListener mListener = MultiThreadListener.ofSingle(listener);
		EnumSet<TripleComponentOrder> toReIndex = EnumSet.noneOf(TripleComponentOrder.class);
		for (TripleComponentOrder order : TripleComponentOrder.values()) {
			if (order == TripleComponentOrder.Unknown || order == this.order) {
				continue;
			}

			Path subIndexPath = BitmapTriplesIndexFile.getIndexPath(fileLocation, order);

			try (FileChannel channel = FileChannel.open(subIndexPath, StandardOpenOption.READ)) {
				// load from the path...

				BitmapTriplesIndex idx = BitmapTriplesIndexFile.map(subIndexPath, channel, this, allowOldOthers);
				BitmapTriplesIndex old = indexes.put(order, idx);
				indexesMask |= idx.getOrder().mask;
				if (old != null) {
					log.warn("an index is using a bad order old:{} cur:{} new:{}", old.getOrder(), order,
							idx.getOrder());
				}
				IOUtil.closeQuietly(old);
			} catch (NoSuchFileException | SignatureIOException ignore) {
				// no index with this name
				if (!askedOrders.contains(order)) {
					continue; // not asked by the user, we can ignore
				}
				// asked by the user, this is an error, we should index it
				toReIndex.add(order);
			}
		}

		for (TripleComponentOrder order : toReIndex) {
			Path subIndexPath = BitmapTriplesIndexFile.getIndexPath(fileLocation, order);

			// (re)generate the file

			// check if we can avoid sorting the subject layer
			BitmapTriplesIndex origin = switch (order) {
			case SPO -> indexes.get(TripleComponentOrder.SOP);
			case SOP -> indexes.get(TripleComponentOrder.SPO);
			case POS -> indexes.get(TripleComponentOrder.PSO);
			case PSO -> indexes.get(TripleComponentOrder.POS);
			case OSP -> indexes.get(TripleComponentOrder.OPS);
			case OPS -> indexes.get(TripleComponentOrder.OSP);
			default -> throw new IllegalArgumentException("Invalid order: " + order);
			};
			if (origin == null) {
				origin = this; // use bitmaptriples by default
			}

			StopWatch sw = new StopWatch();
			log.debug("generate other idx {}->{}", origin.getOrder(), order);
			BitmapTriplesIndexFile.generateIndex(this, origin, subIndexPath, order, spec, mListener);
			log.debug("end generate other idx {}->{} in {}", origin.getOrder(), order, sw.stopAndShow());

			try (FileChannel channel = FileChannel.open(subIndexPath, StandardOpenOption.READ)) {
				// load from the path...
				BitmapTriplesIndex idx = BitmapTriplesIndexFile.map(subIndexPath, channel, this, allowOldOthers);
				BitmapTriplesIndex old = indexes.put(order, idx);
				indexesMask |= order.mask;
				if (old != null) {
					log.warn("an index is using a bad order old:{} cur:{} new:{} after exception", old.getOrder(),
							order, idx.getOrder());
				}
				IOUtil.closeQuietly(old); // should be null?
			} catch (NoSuchFileException ex2) {
				throw new IOException("index not generated", ex2);
			}
		}
	}

	@Override
	public TripleComponentOrder getOrder() {
		return this.order;
	}

	public Sequence getIndexZ() {
		return indexZ;
	}

	@Override
	public Sequence getSeqY() {
		return seqY;
	}

	@Override
	public Sequence getSeqZ() {
		return seqZ;
	}

	@Override
	public AdjacencyList getAdjacencyListY() {
		return adjY;
	}

	@Override
	public AdjacencyList getAdjacencyListZ() {
		return adjZ;
	}

	public AdjacencyList getAdjacencyListIndex() {
		return adjIndex;
	}

	@Override
	public Bitmap getBitmapY() {
		return bitmapY;
	}

	@Override
	public Bitmap getBitmapZ() {
		return bitmapZ;
	}

	public Bitmap getBitmapIndex() {
		return bitmapIndexZ;
	}

	public static class CreateOnUsePath implements Closeable {
		boolean mkdir;
		Path path;

		private CreateOnUsePath() {
			this(null);
		}

		private CreateOnUsePath(Path path) {
			this.path = path;
		}

		public Path createOrGetPath() throws IOException {
			if (path == null) {
				path = Files.createTempDirectory("bitmapTriple");
				mkdir = true;
			} else {
				Files.createDirectories(path);
			}
			return path;
		}

		@Override
		public void close() throws IOException {
			if (mkdir) {
				PathUtils.deleteDirectory(path);
			}
		}
	}

	public MultiLayerBitmap getQuadInfoAG() {
		throw new UnsupportedOperationException("Cannot get quad info from a BitmapTriples");
	}
}

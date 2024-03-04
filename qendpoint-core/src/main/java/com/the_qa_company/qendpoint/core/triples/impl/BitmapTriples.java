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
import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author mario.arias
 */
public class BitmapTriples implements TriplesPrivate, BitmapTriplesIndex {
	private static final Logger log = LoggerFactory.getLogger(BitmapTriples.class);

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
			return new BitmapTriplesIterator(this, pattern);
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

			return new BitmapTriplesIterator(idx, pattern);
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

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#getNumberOfElements()
	 */
	@Override
	public long getNumberOfElements() {
		if (isClosed)
			return 0;
		return seqZ.getNumberOfElements();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.Triples#size()
	 */
	@Override
	public long size() {
		if (isClosed)
			return 0;
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
		DiskIndexSort sort = new DiskIndexSort(CloseSuppressPath.of(diskLocation).resolve("chunks"),
				new AsyncIteratorFetcher<>(new ObjectAdjReader(seqZ, seqY, bitmapZ)), listener, bufferSize, chunkSize,
				k, Comparator.<Pair>comparingLong(p -> p.object).thenComparingLong(p -> p.predicate));

		// Serialize
		DynamicSequence indexZ = null;
		ModifiableBitmap bitmapIndexZ = null;
		DynamicSequence predCount = null;

		global.reset();

		try {
			try {
				ExceptionIterator<Pair, IOException> sortedPairs = sort.sort(workers);

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

	private void createIndexObjectMemoryEfficient() throws IOException {
		Path diskLocation;
		if (diskSequence) {
			diskLocation = diskSequenceLocation.createOrGetPath();
		} else {
			diskLocation = null;
		}

		StopWatch global = new StopWatch();
		StopWatch st = new StopWatch();

		// Count the number of appearances of each object
		long maxCount = 0;
		long numDifferentObjects = 0;
		long numReservedObjects = 8192;
		ModifiableBitmap bitmapIndex = null;
		DynamicSequence predCount = null;
		DynamicSequence objectArray = null;

		try {
			try (DynamicSequence objectCount = createSequence64(diskLocation, "objectCount",
					BitUtil.log2(seqZ.getNumberOfElements()), numReservedObjects)) {
				for (long i = 0; i < seqZ.getNumberOfElements(); i++) {
					long val = seqZ.get(i);
					if (val == 0) {
						throw new RuntimeException("ERROR: There is a zero value in the Z level.");
					}
					if (numReservedObjects < val) {
						while (numReservedObjects < val) {
							numReservedObjects <<= 1;
						}
						objectCount.resize(numReservedObjects);
					}
					if (numDifferentObjects < val) {
						numDifferentObjects = val;
					}

					long count = objectCount.get(val - 1) + 1;
					maxCount = Math.max(count, maxCount);
					objectCount.set(val - 1, count);
				}
				log.info("Count Objects in {} Max was: {}", st.stopAndShow(), maxCount);
				st.reset();

				// Calculate bitmap that separates each object sublist.
				bitmapIndex = createBitmap375(diskLocation, "bitmapIndex", seqZ.getNumberOfElements());
				long tmpCount = 0;
				for (long i = 0; i < numDifferentObjects; i++) {
					tmpCount += objectCount.get(i);
					bitmapIndex.set(tmpCount - 1, true);
				}
				bitmapIndex.set(seqZ.getNumberOfElements() - 1, true);
				log.info("Bitmap in {}", st.stopAndShow());
			}
			st.reset();

			objectArray = createSequence64(diskLocation, "objectArray", BitUtil.log2(seqY.getNumberOfElements()),
					seqZ.getNumberOfElements(), true);
			objectArray.resize(seqZ.getNumberOfElements());

			// Copy each object reference to its position
			try (DynamicSequence objectInsertedCount = createSequence64(diskLocation, "objectInsertedCount",
					BitUtil.log2(maxCount), numDifferentObjects)) {
				objectInsertedCount.resize(numDifferentObjects);

				for (long i = 0; i < seqZ.getNumberOfElements(); i++) {
					long objectValue = seqZ.get(i);
					long posY = i > 0 ? bitmapZ.rank1(i - 1) : 0;

					long insertBase = objectValue == 1 ? 0 : bitmapIndex.select1(objectValue - 1) + 1;
					long insertOffset = objectInsertedCount.get(objectValue - 1);
					objectInsertedCount.set(objectValue - 1, insertOffset + 1);

					objectArray.set(insertBase + insertOffset, posY);
				}
				log.info("Object references in {}", st.stopAndShow());
			}
			st.reset();

			long object = 1;
			long first = 0;
			long last = bitmapIndex.selectNext1(first) + 1;
			do {
				long listLen = last - first;

				// Sublists of one element do not need to be sorted.

				// Hard-coded size 2 for speed (They are quite common).
				if (listLen == 2) {
					long aPos = objectArray.get(first);
					long a = seqY.get(aPos);
					long bPos = objectArray.get(first + 1);
					long b = seqY.get(bPos);
					if (a > b) {
						objectArray.set(first, bPos);
						objectArray.set(first + 1, aPos);
					}
				} else if (listLen > 2) {
					class Pair {
						Long valueY;
						Long positionY;

						@Override
						public String toString() {
							return String.format("%d %d", valueY, positionY);
						}
					}

					// FIXME: Sort directly without copying?
					ArrayList<Pair> list = new ArrayList<>((int) listLen);

					// Create temporary list of (position, predicate)
					for (long i = first; i < last; i++) {
						Pair p = new Pair();
						p.positionY = objectArray.get(i);
						p.valueY = seqY.get(p.positionY);
						list.add(p);
					}

					// Sort
					list.sort((Pair o1, Pair o2) -> {
						if (o1.valueY.equals(o2.valueY)) {
							return o1.positionY.compareTo(o2.positionY);
						}
						return o1.valueY.compareTo(o2.valueY);
					});

					// Copy back
					for (long i = first; i < last; i++) {
						Pair pair = list.get((int) (i - first));
						objectArray.set(i, pair.positionY);
					}
				}

				first = last;
				last = bitmapIndex.selectNext1(first) + 1;
				object++;
			} while (object <= numDifferentObjects);

			log.info("Sort object sublists in {}", st.stopAndShow());
			st.reset();

			// Count predicates
			predCount = createSequence64(diskLocation, "predCount", BitUtil.log2(seqY.getNumberOfElements()), 0);
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
						if (predCount != null) {
							predCount.close();
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

			if ((i % 100000) == 0) {
				System.out.println("Processed: " + i + " objects out of " + total);
			}
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

			if ((i % 100000) == 0) {
				System.out.println("Serialized: " + i + " lists out of " + total);
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

		String indexMethod = specIndex.get(HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_KEY,
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_RECOMMENDED);
		switch (indexMethod) {
		case HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_RECOMMENDED,
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_OPTIMIZED ->
			createIndexObjectMemoryEfficient();
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
		for (TripleComponentOrder order : TripleComponentOrder.values()) {
			if (order == TripleComponentOrder.Unknown || order == this.order) {
				continue;
			}

			Path subIndexPath = BitmapTriplesIndexFile.getIndexPath(fileLocation, order);

			try (FileChannel channel = FileChannel.open(subIndexPath, StandardOpenOption.READ)) {
				// load from the path...

				BitmapTriplesIndex idx = BitmapTriplesIndexFile.map(subIndexPath, channel, this);
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
				// generate the file
				BitmapTriplesIndexFile.generateIndex(this, subIndexPath, order, spec, mListener);
				try (FileChannel channel = FileChannel.open(subIndexPath, StandardOpenOption.READ)) {
					// load from the path...
					BitmapTriplesIndex idx = BitmapTriplesIndexFile.map(subIndexPath, channel, this);
					BitmapTriplesIndex old = indexes.put(order, idx);
					indexesMask |= order.mask;
					if (old != null) {
						log.warn("an index is using a bad order old:{} cur:{} new:{}", old.getOrder(), order,
								idx.getOrder());
					}
					IOUtil.closeQuietly(old); // should be null?
				} catch (NoSuchFileException ex2) {
					throw new IOException("index not generated", ex2);
				}
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

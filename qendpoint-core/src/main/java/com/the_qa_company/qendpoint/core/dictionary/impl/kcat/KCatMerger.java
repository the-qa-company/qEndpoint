package com.the_qa_company.qendpoint.core.dictionary.impl.kcat;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryFactory;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryKCat;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryType;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.OneReadDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.WriteDictionarySection;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapFilterIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MergeExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.concurrent.SyncSeq;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Class to merge multiple dictionaries into S/O/SH streams with map writing
 *
 * @author Antoine Willerval
 */
public class KCatMerger implements AutoCloseable {
	private static final long SHARED_MASK = 0b01;
	private static final long TYPED_MASK = 0b10;

	final HDT[] hdts;

	private final ProgressListener listener;
	private final CloseSuppressPath[] locations;
	final SyncSeq[] subjectsMaps;
	final SyncSeq[] predicatesMaps;
	final SyncSeq[] objectsMaps;
	final SyncSeq[] graphsMaps;
	private final ExceptionThread catMergerThread;
	final boolean typedHDT;
	final boolean langHDT;
	private final int shift;
	private final String dictionaryType;

	private final PipedCopyIterator<DuplicateBuffer> subjectPipe = new PipedCopyIterator<>();
	private final PipedCopyIterator<DuplicateBuffer> objectPipe = new PipedCopyIterator<>();
	private final PipedCopyIterator<BiDuplicateBuffer> sharedPipe = new PipedCopyIterator<>();
	private final ExceptionIterator<DuplicateBuffer, RuntimeException> sortedSubject;
	private final ExceptionIterator<DuplicateBuffer, RuntimeException> sortedObject;
	private final ExceptionIterator<DuplicateBuffer, RuntimeException> sortedPredicates;
	private final Map<ByteString, ExceptionIterator<DuplicateBuffer, RuntimeException>> sortedSubSections;
	private final ExceptionIterator<DuplicateBuffer, RuntimeException> sortedGraphs;

	private final long estimatedSizeP;
	private final long estimatedSizeG;
	private final AtomicLong countTyped = new AtomicLong();
	private final AtomicLong countShared = new AtomicLong();
	private final AtomicLong countNonTyped = new AtomicLong();
	final AtomicLong[] countSubject;
	final AtomicLong[] countObject;

	private final WriteDictionarySection sectionSubject;
	private final WriteDictionarySection sectionShared;
	private final WriteDictionarySection sectionObject;
	private final WriteDictionarySection sectionPredicate;
	private final WriteDictionarySection sectionGraph;
	private final Map<ByteString, WriteDictionarySection> sectionSub;
	private final Map<ByteString, Integer> typeId = new HashMap<>();
	private boolean running;

	/**
	 * Create KCatMerger
	 *
	 * @param hdts           the hdts to cat
	 * @param location       working location
	 * @param listener       listener to log the state
	 * @param bufferSize     buffer size
	 * @param dictionaryType dictionary type
	 * @param quad           quad
	 * @param spec           spec to config the HDT
	 * @throws java.io.IOException io exception
	 */
	public KCatMerger(HDT[] hdts, BitmapTriple[] deletedTriple, CloseSuppressPath location, ProgressListener listener,
			int bufferSize, String dictionaryType, boolean quad, HDTOptions spec) throws IOException {
		this.hdts = hdts;
		this.listener = listener;
		this.dictionaryType = dictionaryType;

		DictionaryKCat[] cats = new DictionaryKCat[hdts.length];
		subjectsMaps = new SyncSeq[hdts.length];
		predicatesMaps = new SyncSeq[hdts.length];
		objectsMaps = new SyncSeq[hdts.length];
		graphsMaps = quad ? new SyncSeq[hdts.length] : null;
		int locationDelta = quad ? 4 : 3;
		locations = new CloseSuppressPath[hdts.length * locationDelta];

		countSubject = IntStream.range(0, hdts.length).mapToObj(i -> new AtomicLong()).toArray(AtomicLong[]::new);
		countObject = IntStream.range(0, hdts.length).mapToObj(i -> new AtomicLong()).toArray(AtomicLong[]::new);

		long sizeS = 0;
		long sizeP = 0;
		long sizeO = 0;
		long sizeONoTyped = 0;
		long sizeShared = 0;
		long sizeG = 0;

		// if this HDT is typed, we don't have to allocate 1 bit / node to note
		// a typed node
		DictionaryType dictTypeE = DictionaryType.fromDictionaryType(dictionaryType);
		this.langHDT = dictTypeE.countLangs();
		this.typedHDT = dictTypeE.countTypes();

		if (typedHDT) {
			shift = 2;
		} else {
			shift = 1;
		}

		Map<ByteString, PreIndexSection[]> subSections = new TreeMap<>();

		for (int i = 0; i < cats.length; i++) {
			DictionaryKCat cat = DictionaryFactory.createDictionaryKCat(hdts[i].getDictionary());

			// compute max allocated sizes
			sizeS += cat.countSubjects();
			sizeP += cat.countPredicates();
			sizeO += cat.countObjects();
			sizeG += cat.countGraphs();
			DictionarySection objectSection = cat.getObjectSection();
			sizeONoTyped += objectSection == null ? 0 : objectSection.getNumberOfElements();
			sizeShared += cat.countShared();

			long start = 1L + cat.typedShift();

			// compute allocated sizes for HDT with sub sections
			for (Map.Entry<CharSequence, DictionarySection> e : cat.getSubSections().entrySet()) {
				CharSequence key = e.getKey();
				DictionarySection section = e.getValue();

				PreIndexSection[] sections = subSections.computeIfAbsent(ByteString.of(key),
						k -> new PreIndexSection[cats.length]);
				sections[i] = new PreIndexSection(start, section);
				start += section.getNumberOfElements();
			}
			cats[i] = cat;
		}

		this.estimatedSizeP = sizeP;
		this.estimatedSizeG = sizeG;

		try {
			// create maps, allocate more bits for the shared part
			int numbitsS = BitUtil.log2(sizeS + 1 + sizeShared) + 1 + shift;
			int numbitsP = BitUtil.log2(sizeP + 1);
			int numbitsO = BitUtil.log2(sizeO + 1 + sizeShared) + 1 + shift;
			int numbitsG = BitUtil.log2(sizeG + 1);
			for (int i = 0; i < cats.length; i++) {
				DictionaryKCat cat = cats[i];
				subjectsMaps[i] = new SyncSeq(
						new SequenceLog64BigDisk((locations[i * locationDelta] = location.resolve("subjectsMap_" + i))
								.toAbsolutePath().toString(), numbitsS, cat.countSubjects() + 1));
				predicatesMaps[i] = new SyncSeq(new SequenceLog64BigDisk(
						(locations[i * locationDelta + 1] = location.resolve("predicatesMap_" + i)).toAbsolutePath()
								.toString(),
						numbitsP, cat.countPredicates() + 1));
				objectsMaps[i] = new SyncSeq(new SequenceLog64BigDisk(
						(locations[i * locationDelta + 2] = location.resolve("objectsMap_" + i)).toAbsolutePath()
								.toString(),
						numbitsO, cat.countObjects() + 1));
				if (quad) {
					graphsMaps[i] = new SyncSeq(new SequenceLog64BigDisk(
							(locations[i * locationDelta + 3] = location.resolve("graphsMap_" + i)).toAbsolutePath()
									.toString(),
							numbitsG, cat.countGraphs() + 1));
				}
			}

			// merge the subjects/objects/shared from all the HDTs
			sortedSubject = mergeSection(cats,
					(hdtIndex, c) -> createMergeIt(hdtIndex, c.getSubjectSection().getSortedEntries(),
							c.getSharedSection().getSortedEntries(),
							deletedTriple == null || deletedTriple[hdtIndex] == null ? null : deletedTriple[hdtIndex].getSubjects(), c.countShared()))
					.notif(sizeS, 20, "Merge subjects", listener);

			sortedObject = mergeSection(cats, (hdtIndex, c) -> {
				DictionarySection section = c.getObjectSection();
				return createMergeIt(hdtIndex, section == null ? new Iterator<>() {
					@Override
					public boolean hasNext() {
						return false;
					}

					@Override
					public CharSequence next() {
						return null;
					}
				} : section.getSortedEntries(), c.getSharedSection().getSortedEntries(),
						deletedTriple == null || deletedTriple[hdtIndex] == null ? null : deletedTriple[hdtIndex].getObjects(), c.nonTypedShift());
			}).notif(sizeONoTyped, 20, "Merge objects", listener);

			// merge the other sections
			sortedPredicates = mergeSection(cats, (hdtIndex, c) -> {
				ExceptionIterator<? extends CharSequence, RuntimeException> of = ExceptionIterator
						.of(c.getPredicateSection().getSortedEntries());
				if (deletedTriple != null && deletedTriple[hdtIndex] != null) {
					ModifiableBitmap deleteBitmap = deletedTriple[hdtIndex].getPredicates();
					return of.mapFiltered(((element, index) -> {
						if (deleteBitmap.access(index + 1)) {
							return null;
						}
						return new LocatedIndexedNode(hdtIndex, index + 1, ByteString.of(element));
					}));
				} else {
					return of.map(
							((element, index) -> new LocatedIndexedNode(hdtIndex, index + 1, ByteString.of(element))));
				}
			}).notif(sizeP, 20, "Merge predicates", listener);

			sortedGraphs = quad ? mergeSection(cats, (hdtIndex, c) -> {
				ExceptionIterator<? extends CharSequence, RuntimeException> of = ExceptionIterator
						.of(c.getGraphSection().getSortedEntries());
				if (deletedTriple != null && deletedTriple[hdtIndex] != null) {
					ModifiableBitmap deleteBitmap = deletedTriple[hdtIndex].getGraphs();
					return of.mapFiltered(((element, index) -> {
						if (deleteBitmap.access(index + 1)) {
							return null;
						}
						return new LocatedIndexedNode(hdtIndex, index + 1, ByteString.of(element));
					}));
				} else {
					return of.map(
							((element, index) -> new LocatedIndexedNode(hdtIndex, index + 1, ByteString.of(element))));
				}
			}).notif(sizeP, 20, "Merge graphs", listener) : null;

			sortedSubSections = new TreeMap<>();
			// create a merge section for each section
			subSections
					.forEach((key, sections) -> sortedSubSections.put(key, mergeSection(sections, (hdtIndex, pre) -> {
						ExceptionIterator<? extends CharSequence, RuntimeException> of = ExceptionIterator
								.of(pre.getSortedEntries());

						if (deletedTriple != null && deletedTriple[hdtIndex] != null) {
							ModifiableBitmap deleteBitmap = deletedTriple[hdtIndex].getObjects();
							return of.mapFiltered(((element, index) -> {
								if (deleteBitmap.access(pre.getStart() + index)) {
									return null;
								}
								return new LocatedIndexedNode(hdtIndex, pre.getStart() + index, ByteString.of(element));
							}));
						} else {
							return of.map(((element, index) -> new LocatedIndexedNode(hdtIndex, pre.getStart() + index,
									ByteString.of(element))));
						}
					}).notif(Arrays.stream(sections)
							.mapToLong(s -> s == null || s.section == null ? 0 : s.section.getNumberOfElements()).sum(),
							20, "Merge typed objects", listener)));

			// convert the dupe buffer streams to byte string streams

			Iterator<ByteString> subject = subjectPipe.mapWithId((db, id) -> {
				long header = withEmptyHeader(id + 1);
				db.stream().forEach(node -> {
					SyncSeq map = subjectsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous subject value";
					map.set(node.getIndex(), header);
					countSubject[node.getHdt()].incrementAndGet();
				});
				return db.peek();
			});

			Iterator<ByteString> object = objectPipe.mapWithId((db, id) -> {
				long header = withEmptyHeader(id + 1);
				countNonTyped.incrementAndGet();
				db.stream().forEach(node -> {
					SyncSeq map = objectsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous object value";
					assert node.getIndex() >= 1 && node.getIndex() <= hdts[node.getHdt()].getDictionary().getNobjects();
					map.set(node.getIndex(), header);
					countObject[node.getHdt()].incrementAndGet();
				});
				return db.peek();
			});

			// left = subjects
			// right = objects
			Iterator<ByteString> shared = sharedPipe.mapWithId((bdb, id) -> {
				long header = withSharedHeader(id + 1);
				countShared.incrementAndGet();
				// left = subjects
				bdb.getLeft().stream().forEach(node -> {
					SyncSeq map = subjectsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous subject value";
					map.set(node.getIndex(), header);
					countSubject[node.getHdt()].incrementAndGet();
				});
				// right = objects
				bdb.getRight().stream().forEach(node -> {
					SyncSeq map = objectsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous object value";
					assert node.getIndex() >= 1 && node.getIndex() <= hdts[node.getHdt()].getDictionary().getNobjects();
					map.set(node.getIndex(), header);
					countObject[node.getHdt()].incrementAndGet();
				});
				return bdb.peek();
			});

			sectionSubject = new WriteDictionarySection(spec, location.resolve("sortedSubject"), bufferSize);
			sectionShared = new WriteDictionarySection(spec, location.resolve("sortedShared"), bufferSize);
			sectionObject = new WriteDictionarySection(spec, location.resolve("sortedObject"), bufferSize);
			sectionPredicate = new WriteDictionarySection(spec, location.resolve("sortedPredicate"), bufferSize);
			sectionGraph = quad ? new WriteDictionarySection(spec, location.resolve("sortedGraph"), bufferSize) : null;
			sectionSub = new TreeMap<>();
			sortedSubSections.keySet().forEach((key) -> sectionSub.put(key,
					new WriteDictionarySection(spec, location.resolve("sortedSub" + getTypeId(key)), bufferSize)));

			catMergerThread = new ExceptionThread(this::runSharedCompute, "KCatMergerThreadShared")
					.attach(new ExceptionThread(this::runSubSectionCompute, "KCatMergerThreadSubSection"))
					.attach(new ExceptionThread(createWriter(sectionSubject, sizeS, subject),
							"KCatMergerThreadWriterS"))
					.attach(new ExceptionThread(createWriter(sectionShared, sizeS + sizeO - sizeShared, shared),
							"KCatMergerThreadWriterSH"))
					.attach(new ExceptionThread(createWriter(sectionObject, sizeO, object), "KCatMergerThreadWriterO"));
		} catch (Throwable t) {
			try {
				throw t;
			} finally {
				close();
			}
		}
	}

	private static ExceptionIterator<LocatedIndexedNode, RuntimeException> createMergeIt(int hdtIndex,
			Iterator<? extends CharSequence> subjectObject, Iterator<? extends CharSequence> shared,
			Bitmap deleteBitmap, long sharedCount) {
		if (deleteBitmap != null) {
			return MergeExceptionIterator.buildOfTree(List.of(MapFilterIterator.of(subjectObject, (element, index) -> {
				if (deleteBitmap.access(sharedCount + index + 1)) {
					return null;
				}
				return new LocatedIndexedNode(hdtIndex, sharedCount + index + 1, ByteString.of(element));
			}).asExceptionIterator(), MapFilterIterator.of(shared, (element, index) -> {
				if (deleteBitmap.access(index + 1)) {
					return null;
				}
				return new LocatedIndexedNode(hdtIndex, index + 1, ByteString.of(element));
			}).asExceptionIterator()));
		} else {
			return MergeExceptionIterator.buildOfTree(List.of(
					MapIterator.of(subjectObject,
							(element, index) -> new LocatedIndexedNode(hdtIndex, sharedCount + index + 1,
									ByteString.of(element)))
							.asExceptionIterator(),
					MapIterator.of(shared,
							(element, index) -> new LocatedIndexedNode(hdtIndex, index + 1, ByteString.of(element)))
							.asExceptionIterator()));
		}
	}

	/**
	 * create a sorted LocatedIndexedNode iterator from an array of sections
	 *
	 * @param sections the sections
	 * @return iterator
	 */
	public static <T> DuplicateBufferIterator<RuntimeException> mergeSection(T[] sections, MergerFunction<T> mapper) {
		return new DuplicateBufferIterator<>(MergeExceptionIterator.buildOfTree((Integer hdtIndex, T e) -> {
			if (e == null) {
				// empty section (not defined for this HDT)
				return ExceptionIterator.empty();
			}
			// convert all the entries into located nodes
			return mapper.apply(hdtIndex, e);
		}, LocatedIndexedNode::compareTo, Arrays.asList(sections), 0, sections.length), sections.length);
	}

	/**
	 * get an UID for a type
	 *
	 * @param str the type
	 * @return UID
	 */
	public int getTypeId(ByteString str) {
		return typeId.computeIfAbsent(str, (key) -> typeId.size());
	}

	/**
	 * add a typed header to this value
	 *
	 * @param value value
	 * @return header value
	 * @see #withEmptyHeader(long)
	 * @see #withSharedHeader(long)
	 */
	public long withTypedHeader(long value) {
		assert value != 0 : "value can't be 0!";
		return (value << shift) | TYPED_MASK;
	}

	/**
	 * add a shared header to this value
	 *
	 * @param value value
	 * @return header value
	 * @see #withEmptyHeader(long)
	 * @see #withTypedHeader(long)
	 */
	public long withSharedHeader(long value) {
		assert value != 0 : "value can't be 0!";
		return (value << shift) | SHARED_MASK;
	}

	/**
	 * add a header to this value
	 *
	 * @param value value
	 * @return header value
	 * @see #withTypedHeader(long)
	 * @see #withSharedHeader(long)
	 */
	public long withEmptyHeader(long value) {
		assert value != 0 : "value can't be 0!";
		return value << shift;
	}

	boolean assertReadCorrectly() {
		for (int i = 0; i < hdts.length; i++) {
			HDT hdt = hdts[i];
			assert countObject[i].get() == hdt.getDictionary().getNobjects();
			assert countSubject[i].get() == hdt.getDictionary().getNsubjects();
		}
		return true;
	}

	/**
	 * test if a header value is shared
	 *
	 * @param headerValue header value
	 * @return true if the header is shared, false otherwise
	 */
	public boolean isShared(long headerValue) {
		return (headerValue & SHARED_MASK) != 0;
	}

	/**
	 * test if a header value is typed
	 *
	 * @param headerValue header value
	 * @return true if the header is typed, false otherwise
	 */
	public boolean isTyped(long headerValue) {
		return typedHDT && (headerValue & TYPED_MASK) != 0;
	}

	/**
	 * wait for the merger to complete
	 *
	 * @throws InterruptedException thread interruption
	 */
	public DictionaryPrivate buildDictionary() throws InterruptedException {
		synchronized (this) {
			if (!running) {
				startMerger();
			}
		}
		catMergerThread.joinAndCrashIfRequired();

		return DictionaryFactory.createWriteDictionary(dictionaryType, null, getSectionSubject(), getSectionPredicate(),
				getSectionObject(), getSectionShared(), getSectionSub(), getSectionGraph());
	}

	private void runSharedCompute() {
		// merge the sections
		try {
			sharedLoop:
			while (sortedObject.hasNext() && sortedSubject.hasNext()) {
				// last was a shared node
				DuplicateBuffer newSubject = sortedSubject.next();
				DuplicateBuffer newObject = sortedObject.next();
				int comp = newSubject.compareTo(newObject);
				while (comp != 0) {
					if (comp < 0) {
						subjectPipe.addElement(newSubject.trim());
						if (!sortedSubject.hasNext()) {
							// no more subjects, send the current object and
							// break the shared loop
							objectPipe.addElement(newObject.trim());
							break sharedLoop;
						}
						newSubject = sortedSubject.next();
					} else {
						objectPipe.addElement(newObject.trim());
						if (!sortedObject.hasNext()) {
							// no more objects, send the current subject and
							// break the shared loop
							subjectPipe.addElement(newSubject.trim());
							break sharedLoop;
						}
						newObject = sortedObject.next();
					}
					comp = newSubject.compareTo(newObject);
				}

				// shared element
				sharedPipe.addElement(newSubject.trim().asBi(newObject.trim()));
			}

			// at least one iterator is empty, closing the shared pipe
			sharedPipe.closePipe();
			// do we have subjects?
			while (sortedSubject.hasNext()) {
				subjectPipe.addElement(sortedSubject.next().trim());
			}
			subjectPipe.closePipe();
			// do we have objects?
			while (sortedObject.hasNext()) {
				objectPipe.addElement(sortedObject.next().trim());
			}
			objectPipe.closePipe();
		} catch (Throwable t) {
			objectPipe.closePipe(t);
			subjectPipe.closePipe(t);
			sharedPipe.closePipe(t);
			throw t;
		}
	}

	private void runSubSectionCompute() {
		// convert all the sections

		// load predicates
		sectionPredicate.load(new OneReadDictionarySection(sortedPredicates.map((db, id) -> {
			db.stream().forEach(node -> {
				SyncSeq map = predicatesMaps[node.getHdt()];
				assert map.get(node.getIndex()) == 0 : "overwriting previous predicate value";
				map.set(node.getIndex(), id + 1);
			});
			return db.peek();
		}).asIterator(), estimatedSizeP), null);

		// load graphs
		if (sectionGraph != null) {
			sectionGraph.load(new OneReadDictionarySection(sortedGraphs.map((db, id) -> {
				db.stream().forEach(node -> {
					SyncSeq map = graphsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous graph value";
					map.set(node.getIndex(), id + 1);
				});
				return db.peek();
			}).asIterator(), estimatedSizeG), null);
		}

		long shift = 1L;
		// load data typed sections
		for (Map.Entry<ByteString, WriteDictionarySection> e : sectionSub.entrySet()) {
			ByteString key = e.getKey();
			WriteDictionarySection section = e.getValue();

			ExceptionIterator<DuplicateBuffer, RuntimeException> bufferIterator = sortedSubSections.get(key);

			final long currentShift = shift;
			section.load(new OneReadDictionarySection(bufferIterator.map((db, id) -> {
				long headerID = withTypedHeader(id + currentShift);
				countTyped.incrementAndGet();
				db.stream().forEach(node -> {
					SyncSeq map = objectsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous object value";
					assert node.getIndex() >= 1 && node.getIndex() <= hdts[node.getHdt()].getDictionary().getNobjects();
					map.set(node.getIndex(), headerID);
					countObject[node.getHdt()].incrementAndGet();
				});
				return db.peek();
			}).asIterator(), estimatedSizeP), null);
			shift += section.getNumberOfElements();
		}
	}

	private ExceptionThread.ExceptionRunnable createWriter(DictionarySectionPrivate sect, long size,
			Iterator<ByteString> iterator) {
		// convert all the sections
		return () -> sect.load(new OneReadDictionarySection(iterator, size), listener);
	}

	@Override
	public void close() throws IOException {
		try {
			if (catMergerThread != null) {
				catMergerThread.joinAndCrashIfRequired();
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} finally {
			Closer.closeAll(sectionSubject, sectionPredicate, sectionGraph, sectionObject, sectionShared, sectionSub,
					subjectsMaps, predicatesMaps, graphsMaps, objectsMaps, locations);
		}
	}

	/**
	 * remove the header of a header id
	 *
	 * @param headerID header id
	 * @return id
	 */
	public long removeHeader(long headerID) {
		return headerID >>> shift;
	}

	/**
	 * extract the subject from an HDT
	 *
	 * @param hdtIndex the HDT index
	 * @param oldID    the ID in the HDT triples
	 * @return ID in the new HDT
	 */
	public long extractSubject(int hdtIndex, long oldID) {
		long headerID = subjectsMaps[hdtIndex].get(oldID);
		if (isShared(headerID)) {
			return headerID >>> shift;
		}
		return (headerID >>> shift) + countShared.get();
	}

	/**
	 * extract the predicate from an HDT
	 *
	 * @param hdtIndex the HDT index
	 * @param oldID    the ID in the HDT triples
	 * @return ID in the new HDT
	 */
	public long extractPredicate(int hdtIndex, long oldID) {
		return predicatesMaps[hdtIndex].get(oldID);
	}

	/**
	 * extract the graph from an HDTq
	 *
	 * @param hdtIndex the HDT index
	 * @param oldID    the ID in the HDT triples
	 * @return ID in the new HDT
	 */
	public long extractGraph(int hdtIndex, long oldID) {
		return graphsMaps[hdtIndex].get(oldID);
	}

	/**
	 * extract the object from an HDT
	 *
	 * @param hdtIndex the HDT index
	 * @param oldID    the ID in the HDT triples
	 * @return ID in the new HDT
	 */
	public long extractObject(int hdtIndex, long oldID) {
		long headerID = objectsMaps[hdtIndex].get(oldID);
		if (isShared(headerID)) {
			return headerID >>> shift;
		}
		if (isTyped(headerID)) {
			if (langHDT) {
				// in a MSDL the NDT section is before the DT/LG sections
				return (headerID >>> shift) + countShared.get() + countNonTyped.get();
			}
			return (headerID >>> shift) + countShared.get();
		}
		if (langHDT) {
			return (headerID >>> shift) + countShared.get();
		}
		return (headerID >>> shift) + countShared.get() + countTyped.get();
	}

	/**
	 * copy into a new TripleID the mapped version of a tripleID
	 *
	 * @param hdtIndex the origin HDT of this tripleID
	 * @param id       the tripleID
	 * @return mapped tripleID
	 */
	public TripleID extractMapped(int hdtIndex, TripleID id) {
		TripleID mapped;
		if (graphsMaps != null) {
			mapped = new TripleID(extractSubject(hdtIndex, id.getSubject()),
					extractPredicate(hdtIndex, id.getPredicate()), extractObject(hdtIndex, id.getObject()),
					extractGraph(hdtIndex, id.getGraph()));
		} else {
			mapped = new TripleID(extractSubject(hdtIndex, id.getSubject()),
					extractPredicate(hdtIndex, id.getPredicate()), extractObject(hdtIndex, id.getObject()));
		}
		assert mapped.isValid() : "mapped to empty triples! " + id + " => " + mapped;
		return mapped;
	}

	/**
	 * @return the count of shared elements
	 */
	public long getCountShared() {
		return countShared.get();
	}

	/**
	 * @return subject section
	 */
	public DictionarySectionPrivate getSectionSubject() {
		return sectionSubject;
	}

	/**
	 * @return shared section
	 */
	public DictionarySectionPrivate getSectionShared() {
		return sectionShared;
	}

	/**
	 * @return object section
	 */
	public DictionarySectionPrivate getSectionObject() {
		return sectionObject;
	}

	/**
	 * @return predicate section
	 */
	public DictionarySectionPrivate getSectionPredicate() {
		return sectionPredicate;
	}

	/**
	 * @return graph section
	 */
	public WriteDictionarySection getSectionGraph() {
		return sectionGraph;
	}

	/**
	 * @return sub sections
	 */
	public TreeMap<ByteString, DictionarySectionPrivate> getSectionSub() {
		TreeMap<ByteString, DictionarySectionPrivate> sub = new TreeMap<>(sectionSub);
		sub.put(LiteralsUtils.NO_DATATYPE, getSectionObject());
		return sub;
	}

	/**
	 * start the merger threads
	 */
	public synchronized void startMerger() {
		if (running) {
			throw new IllegalArgumentException("KCatMerger is already running!");
		}
		running = true;

		catMergerThread.startAll();
	}

	/**
	 * @return if the merge is handling quads
	 */
	public boolean isQuad() {
		return graphsMaps != null;
	}

	static class BiDuplicateBuffer implements Comparable<BiDuplicateBuffer> {
		private final DuplicateBuffer left;
		private final DuplicateBuffer right;

		public BiDuplicateBuffer(DuplicateBuffer left, DuplicateBuffer right) {
			this.left = Objects.requireNonNull(left, "left buffer can't be null!");
			this.right = Objects.requireNonNull(right, "right buffer can't be null!");
			assert left.isEmpty() || right.isEmpty() || left.peek().equals(right.peek())
					: "Can't have heterogeneous bi dupe buffer";
		}

		public DuplicateBuffer getLeft() {
			return left;
		}

		public DuplicateBuffer getRight() {
			return right;
		}

		public boolean isEmpty() {
			return getLeft().isEmpty() && getRight().isEmpty();
		}

		public ByteString peek() {
			if (!left.isEmpty()) {
				return left.peek();
			}
			if (!right.isEmpty()) {
				return right.peek();
			}
			return null;
		}

		@Override
		public int compareTo(BiDuplicateBuffer o) {
			return peek().compareTo(o.peek());
		}
	}

	static class DuplicateBuffer implements Comparable<DuplicateBuffer> {
		private final LocatedIndexedNode[] buffer;
		private int used;

		public DuplicateBuffer(int bufferSize) {
			this.buffer = new LocatedIndexedNode[bufferSize];
		}

		/**
		 * add a node to this buffer
		 *
		 * @param node node
		 * @return if this node was added to the buffer
		 */
		private boolean add(LocatedIndexedNode node) {
			// start case
			if (isEmpty() || buffer[0].getNode().equals(node.getNode())) {
				// we can't have more than buffer size because a source HDT
				// wouldn't be
				// without duplicated or a so/sh conflict
				buffer[used++] = node;
				return true;
			}

			return false;
		}

		/**
		 * convert this buffer to a bi duplicate buffer
		 *
		 * @param other right part
		 * @return BiDuplicateBuffer
		 */
		public BiDuplicateBuffer asBi(DuplicateBuffer other) {
			return new BiDuplicateBuffer(this, other);
		}

		/**
		 * @return if this buffer contains at least one element
		 */
		public boolean isEmpty() {
			return used == 0;
		}

		/**
		 * clear the buffer
		 */
		public void clear() {
			// clear old values
			for (int i = 0; i < used; i++) {
				buffer[i] = null;
			}
			used = 0;
		}

		/**
		 * @return a stream of the current duplicate objects
		 */
		public Stream<LocatedIndexedNode> stream() {
			return Arrays.stream(buffer, 0, used);
		}

		@Override
		public int compareTo(DuplicateBuffer o) {
			if (isEmpty() || o.isEmpty()) {
				throw new IllegalArgumentException("Can't compare empty buffers");
			}
			return buffer[0].compareTo(o.buffer[0]);
		}

		/**
		 * @return a trimmed version of this buffer
		 */
		public DuplicateBuffer trim() {
			DuplicateBuffer other = new DuplicateBuffer(used);
			System.arraycopy(buffer, 0, other.buffer, 0, used);
			other.used = used;
			return other;
		}

		/**
		 * @return the buffered byte string, null if empty
		 */
		public ByteString peek() {
			if (isEmpty()) {
				return null;
			}
			return buffer[0].getNode();
		}

		/**
		 * @return the size of the buffer
		 */
		public int size() {
			return used;
		}
	}

	static class DuplicateBufferIterator<E extends Exception> implements ExceptionIterator<DuplicateBuffer, E> {
		private final ExceptionIterator<LocatedIndexedNode, E> iterator;
		private final DuplicateBuffer buffer;
		private LocatedIndexedNode last;
		private DuplicateBuffer next;

		public DuplicateBufferIterator(ExceptionIterator<LocatedIndexedNode, E> iterator, int bufferSize) {
			this.iterator = iterator;
			buffer = new DuplicateBuffer(bufferSize);
		}

		@Override
		public boolean hasNext() throws E {
			if (next != null) {
				return true;
			}

			// clear previous buffer
			buffer.clear();
			while (true) {
				// load an element from the iterator
				if (last == null) {
					if (!iterator.hasNext()) {
						if (buffer.isEmpty()) {
							return false;
						}
						break;
					}
					last = iterator.next();
				}

				// add the elements from the iterator
				if (!buffer.add(last)) {
					break;
				}
				last = null;
			}

			next = buffer.trim();
			return true;
		}

		@Override
		public DuplicateBuffer next() throws E {
			if (!hasNext()) {
				return null;
			}
			try {
				return next;
			} finally {
				next = null;
			}
		}

	}

	private interface MergerFunction<T> {
		ExceptionIterator<LocatedIndexedNode, RuntimeException> apply(int hdtIndex, T t);
	}

	private static class PreIndexSection {
		long start;
		DictionarySection section;

		public PreIndexSection(long start, DictionarySection section) {
			this.start = start;
			this.section = section;
		}

		public long getStart() {
			return start;
		}

		public DictionarySection getSection() {
			return section;
		}

		public Iterator<? extends CharSequence> getSortedEntries() {
			return getSection().getSortedEntries();
		}
	}
}

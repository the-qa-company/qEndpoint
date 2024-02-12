package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.storage.converter.NodeConverter;
import com.the_qa_company.qendpoint.core.storage.converter.PermutationNodeConverter;
import com.the_qa_company.qendpoint.core.storage.converter.SharedWrapperNodeConverter;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.crc.CRC;
import com.the_qa_company.qendpoint.core.util.crc.CRC16;
import com.the_qa_company.qendpoint.core.util.debug.DebugInjectionPointManager;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.the_qa_company.qendpoint.core.enums.TripleComponentRole.OBJECT;
import static com.the_qa_company.qendpoint.core.enums.TripleComponentRole.PREDICATE;
import static com.the_qa_company.qendpoint.core.enums.TripleComponentRole.SUBJECT;
import static java.lang.String.format;

/**
 * Linker to fetch data from a dataset to another one
 *
 * @author Antoine Willerval
 */
public class QEPMap implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(QEPMap.class);
	static DebugInjectionPointManager.DebugInjectionPoint<QEPMap> endSync = DebugInjectionPointManager.getInstance()
			.registerInjectionPoint(QEPMap.class);
	public static final long SECTION_MAST = 1;
	public static final int SECTION_SHIFT = 1;
	public static final long SECTION_TYPE_SUBJECT = 0;
	public static final long SECTION_TYPE_OBJECT = 1;
	private static final int HEADER_SIZE;
	private static final byte[] MAGIC = "$QML".getBytes(ByteStringUtil.STRING_ENCODING);

	private static final byte CORE_VERSION = 0x10;

	static {
		int headerSize = 0;

		// magic: byte[MAGIC.length]
		headerSize += MAGIC.length * Byte.BYTES;

		// Core map version
		headerSize += Byte.BYTES;

		// id sizes: (ID size + 1) * 2 (+1 for '\0')
		headerSize += (QEPCore.MAX_ID_SIZE + 1) * Byte.BYTES * 2;
		// 4 sections * 2 datasets
		headerSize += Long.BYTES * 4 * 2;

		// id size
		headerSize += Long.BYTES * TripleComponentRole.valuesNoGraph().length;
		// map size
		headerSize += Long.BYTES * TripleComponentRole.valuesNoGraph().length;

		HEADER_SIZE = headerSize;
	}

	record SectionMap(LongArray idSequence1, LongArray mapSequence1, LongArray idSequence2, LongArray mapSequence2,
			long nshared1, long nshared2) implements Closeable {
		LongArray idByNumber(int number) {
			return switch (number) {
			case 0 -> idSequence1;
			case 1 -> idSequence2;
			default -> throw new AssertionError();
			};
		}

		long nsByNumber(int number) {
			return switch (number) {
			case 0 -> nshared1;
			case 1 -> nshared2;
			default -> throw new AssertionError();
			};
		}

		LongArray mapByNumber(int number) {
			return switch (number) {
			case 0 -> mapSequence1;
			case 1 -> mapSequence2;
			default -> throw new AssertionError();
			};
		}

		@Override
		public void close() throws IOException {
			Closer.closeAll(idSequence1, mapSequence1, idSequence2, mapSequence2);
		}
	}

	record DatasetNodeConverter(NodeConverter dataset1to2, NodeConverter dataset2to1) {}

	/**
	 * get the role of a mapped id
	 *
	 * @param mappedId the mapped id
	 * @return role
	 */
	public static TripleComponentRole getRoleOfMapped(long mappedId) {
		return (mappedId & SECTION_MAST) == SECTION_TYPE_SUBJECT ? SUBJECT : OBJECT;
	}

	/**
	 * extract the component id of a mapped id
	 *
	 * @param mappedId mapped id
	 * @return id
	 */
	public static long getIdOfMapped(long mappedId, long countShared) {
		long id = mappedId >>> SECTION_SHIFT;
		if ((mappedId & SECTION_MAST) == SECTION_TYPE_SUBJECT) {
			return id;
		}
		return id + countShared;
	}

	final QEPDataset dataset1;
	final QEPDataset dataset2;

	final Path path;
	final Uid uid;
	final QEPCore core;
	final SectionMap[] maps = new SectionMap[TripleComponentRole.valuesNoGraph().length];
	final boolean useDataset1;
	final DatasetNodeConverter[] nodeConverters = new DatasetNodeConverter[maps.length];

	QEPMap(Path parent, QEPCore core, QEPDataset dataset1, QEPDataset dataset2) {
		this.core = Objects.requireNonNull(core);
		String id1 = dataset1.id();
		String id2 = dataset2.id();

		int c = id1.compareTo(id2);

		if (c == 0) {
			throw new IllegalFormatException("Can't link a dataset to itself!");
		}

		// we use the lower id as our dataset 1 and greater id as our dataset 2

		if (c < 0) {
			this.dataset1 = dataset1;
			this.dataset2 = dataset2;
		} else { // we switch the dataset order
			this.dataset1 = dataset2;
			this.dataset2 = dataset1;
		}

		path = parent.resolve("map-" + getMapId());
		uid = Uid.of(this.dataset1.uid(), this.dataset2.uid());

		// compute the useDataset1 var
		long sizeDataset1 = Arrays.stream(TripleComponentRole.valuesNoGraph())
				.mapToLong(r -> dataset1.dataset().getDictionary().getNSection(r, r == SUBJECT)).sum();
		long sizeDataset2 = Arrays.stream(TripleComponentRole.valuesNoGraph())
				.mapToLong(r -> dataset2.dataset().getDictionary().getNSection(r, r == SUBJECT)).sum();
		useDataset1 = sizeDataset1 < sizeDataset2;
	}

	/**
	 * Sync the linker object
	 *
	 * @throws IOException error while syncing
	 */
	public void sync() throws IOException {
		Path mapHeaderPath = getMapHeaderPath();
		try {
			close();
			// the CRC used to check the header
			CRC crc = new CRC16();
			// create the sync path
			Files.createDirectories(path);
			boolean created = !Files.exists(mapHeaderPath);
			// race condition
			// we open the header file to see if it's actually the right map
			try (FileChannel channel = FileChannel.open(mapHeaderPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			     // FIXME: read the magic+version and then read the correct
			     // header size
			     CloseMappedByteBuffer header = IOUtil.mapChannel(mapHeaderPath, channel,
							FileChannel.MapMode.READ_WRITE, 0, HEADER_SIZE);
			     CloseMappedByteBuffer crcBuffer = IOUtil.mapChannel(mapHeaderPath, channel,
					     FileChannel.MapMode.READ_WRITE, HEADER_SIZE, crc.sizeof())) {
				// store the id and the location to write it after creation
				long[] index1Size = new long[TripleComponentRole.valuesNoGraph().length];
				int[] index1Location = new int[index1Size.length];
				long[] index2Size = new long[TripleComponentRole.valuesNoGraph().length];
				int[] index2Location = new int[index2Size.length];
				// header creation
				{
					int shift = 0;

					if (created) {
						// we write the header
						// write magic
						for (; shift < MAGIC.length; shift++) {
							header.put(shift, MAGIC[shift]);
						}
						header.put(shift++, CORE_VERSION);
						IOUtil.writeCString(header, dataset1.id(), shift);
						shift += QEPCore.MAX_ID_SIZE + 1;
						IOUtil.writeCString(header, dataset2.id(), shift);
						shift += QEPCore.MAX_ID_SIZE + 1;
						Dictionary d1 = dataset1.dataset().getDictionary();
						long nshared1 = d1.getNshared();
						header.putLong(shift, nshared1);
						header.putLong(shift += Long.BYTES, d1.getNsubjects() - nshared1);
						header.putLong(shift += Long.BYTES, d1.getNpredicates());
						header.putLong(shift += Long.BYTES, d1.getNobjects() - nshared1);
						shift += Long.BYTES;

						Dictionary d2 = dataset2.dataset().getDictionary();
						long nshared2 = d2.getNshared();
						header.putLong(shift, nshared2);
						header.putLong(shift += Long.BYTES, d2.getNsubjects() - nshared2);
						header.putLong(shift += Long.BYTES, d2.getNpredicates());
						header.putLong(shift += Long.BYTES, d2.getNobjects() - nshared2);
						shift += Long.BYTES;

						for (int i = 0; i < index1Location.length; i++) {
							index1Location[i] = shift;
							shift += Long.BYTES;
						}
						for (int i = 0; i < index2Location.length; i++) {
							index2Location[i] = shift;
							shift += Long.BYTES;
						}
					} else {
						// we check the magic
						byte[] magicRead = new byte[MAGIC.length];
						header.get(magicRead, 0, magicRead.length);
						for (; shift < MAGIC.length; shift++) {
							if (magicRead[shift] != MAGIC[shift]) {
								throw new IOException("Can't read magic number of dataset linker " + getMapId() + "! " + Arrays.toString(magicRead) + " != " + Arrays.toString(MAGIC));
							}
						}
						byte coreVersion = header.get(shift++);

						if (coreVersion != CORE_VERSION) {
							syncOld(coreVersion);
							return;
						}

						String aid1 = IOUtil.readCString(header, shift, QEPCore.MAX_ID_SIZE + 1);
						shift += QEPCore.MAX_ID_SIZE + 1;
						if (!aid1.equals(dataset1.id())) {
							throw new IOException("read dataset id1=" + aid1 + " but use " + dataset1.id());
						}

						String aid2 = IOUtil.readCString(header, shift, QEPCore.MAX_ID_SIZE + 1);
						shift += QEPCore.MAX_ID_SIZE + 1;
						if (!aid2.equals(dataset2.id())) {
							throw new IOException("read dataset id2=" + aid2 + " but use " + dataset2.id());
						}

						checkHeader(header, shift, "1", this.dataset1);
						shift += Long.BYTES * 4;
						checkHeader(header, shift, "2", this.dataset2);
						shift += Long.BYTES * 4;

						for (int i = 0; i < index1Size.length; i++) {
							index1Location[i] = shift;
							index1Size[i] = header.getLong(shift);
							shift += Long.BYTES;
						}
						for (int i = 0; i < index2Size.length; i++) {
							index2Location[i] = shift;
							index2Size[i] = header.getLong(shift);
							shift += Long.BYTES;
						}

						crc.update(header, 0, HEADER_SIZE);
						if (!crc.readAndCheck(crcBuffer, 0)) {
							throw new CRCException("CRC Error while reading QEPMap header.");
						}
					}
					assert shift == HEADER_SIZE : shift + "!=" + HEADER_SIZE;
				}

				boolean dataset1Base = isMapDataset1Smaller();

				QEPDataset dataset1;
				QEPDataset dataset2;

				if (dataset1Base) {
					dataset1 = this.dataset1;
					dataset2 = this.dataset2;
				} else {
					dataset1 = this.dataset2;
					dataset2 = this.dataset1;
				}

				// predicate mapping generation
				{
					int roleId = PREDICATE.ordinal();

					Path map1OriginPath = getMap1OriginPath(PREDICATE);
					Path map1DestinationPath = getMap1DestinationPath(PREDICATE);

					boolean regen = checkRegen(PREDICATE, created);

					DynamicSequence idSequence1 = null;
					DynamicSequence mapSequence1 = null;
					try {
						long nSection1 = dataset1.dataset().getDictionary().getNSection(PREDICATE);
						long nSection2 = dataset2.dataset().getDictionary().getNSection(PREDICATE);
						int numbitsId1 = BitUtil.log2(nSection1);
						int numbitsId2 = BitUtil.log2(nSection2);
						// add one bit in the map section to store the
						// subject/object type
						if (!regen) {
							idSequence1 = new SequenceLog64BigDisk(map1OriginPath, numbitsId1, index1Size[roleId], true,
									false);
							mapSequence1 = new SequenceLog64BigDisk(map1DestinationPath, numbitsId2, index1Size[roleId],
									true, false);
						} else {
							idSequence1 = new SequenceLog64BigDisk(map1OriginPath, numbitsId1, nSection1, true, true);
							mapSequence1 = new SequenceLog64BigDisk(map1DestinationPath, numbitsId2, nSection1, true,
									true);

							long componentIdCounter = 1;
							long sequenceIndex = 1;

							Iterator<? extends CharSequence> it = dataset1.dataset().getDictionary()
									.stringIterator(PREDICATE);

							while (it.hasNext()) {
								// convert to bytestring to optimise search
								CharSequence component = ByteString.of(it.next());
								long componentId = componentIdCounter++;

								long mappedId = dataset2.dataset().getDictionary().stringToId(component, PREDICATE);

								if (mappedId > 0) {

									// we add the componentId -> mappedId
									// permutation
									idSequence1.set(sequenceIndex, componentId);
									mapSequence1.set(sequenceIndex, mappedId);
									sequenceIndex++;
								}
							}

							// we write the size in the header, same because
							// of the lack of SH/S/O conversion
							header.putLong(index1Location[roleId], sequenceIndex);
							header.putLong(index2Location[roleId], sequenceIndex);

							// we resize our files to reduce space usage
							idSequence1.resize(sequenceIndex);
							mapSequence1.resize(sequenceIndex);
						}

						// using the same maps because the predicates are sorted
						// with the same order on both dataset
						// we reverse their order because in the 2nd context,
						// mapseq2 is the idseq and idseq2 is mapseq
						maps[PREDICATE.ordinal()] = new SectionMap(idSequence1, mapSequence1, mapSequence1, idSequence1,
								0, 0);
					} catch (Throwable t) {
						try {
							Closer.closeAll(idSequence1, mapSequence1);
						} catch (Exception ee) {
							t.addSuppressed(ee);
						} catch (Error err) {
							err.addSuppressed(t);
							throw err;
						}
						throw t;
					}
				}

				{
					Path subjectMap1OriginPath = getMap1OriginPath(SUBJECT);
					Path subjectMap1DestinationPath = getMap1DestinationPath(SUBJECT);
					Path subjectMap2OriginPath = getMap2OriginPath(SUBJECT);
					Path subjectMap2DestinationPath = getMap2DestinationPath(SUBJECT);

					Path objectMap1OriginPath = getMap1OriginPath(OBJECT);
					Path objectMap1DestinationPath = getMap1DestinationPath(OBJECT);
					Path objectMap2OriginPath = getMap2OriginPath(OBJECT);
					Path objectMap2DestinationPath = getMap2DestinationPath(OBJECT);

					boolean regen1 = checkRegen(SUBJECT, created);
					boolean regen2 = checkRegen(OBJECT, created);
					boolean regen = regen1 || regen2;

					DynamicSequence subjectIdSequence1 = null;
					DynamicSequence subjectMapSequence1 = null;
					DynamicSequence subjectIdSequence2 = null;
					DynamicSequence subjectMapSequence2 = null;

					DynamicSequence objectIdSequence1 = null;
					DynamicSequence objectMapSequence1 = null;
					DynamicSequence objectIdSequence2 = null;
					DynamicSequence objectMapSequence2 = null;
					try {
						long subjectNSection1 = dataset1.dataset().getDictionary().getNSection(SUBJECT, true) + 1;
						long subjectNSection2 = dataset2.dataset().getDictionary().getNSection(SUBJECT, true) + 1;
						int subjectNumbitsId1 = BitUtil.log2(subjectNSection1);
						int subjectNumbitsId2 = BitUtil.log2(subjectNSection2);

						long objectNSection1 = dataset1.dataset().getDictionary().getNSection(OBJECT, false) + 1;
						long objectNSection2 = dataset2.dataset().getDictionary().getNSection(OBJECT, false) + 1;
						int objectNumbitsId1 = BitUtil.log2(objectNSection1);
						int objectNumbitsId2 = BitUtil.log2(objectNSection2);

						int sharedNumbits1 = Math.max(subjectNumbitsId1, objectNumbitsId1);
						int sharedNumbits2 = Math.max(subjectNumbitsId2, objectNumbitsId2);
						// add one bit in the map section to store the
						// subject/object type
						if (!regen) {
							subjectIdSequence1 = new SequenceLog64BigDisk(subjectMap1OriginPath, subjectNumbitsId1,
									index1Size[SUBJECT.ordinal()], true, false);
							subjectMapSequence1 = new SequenceLog64BigDisk(subjectMap1DestinationPath,
									sharedNumbits2 + SECTION_SHIFT, index1Size[SUBJECT.ordinal()], true, false);
							subjectIdSequence2 = new SequenceLog64BigDisk(subjectMap2OriginPath, subjectNumbitsId2,
									index2Size[SUBJECT.ordinal()], true, false);
							subjectMapSequence2 = new SequenceLog64BigDisk(subjectMap2DestinationPath,
									sharedNumbits1 + SECTION_SHIFT, index2Size[SUBJECT.ordinal()], true, false);

							objectIdSequence1 = new SequenceLog64BigDisk(objectMap1OriginPath, objectNumbitsId1,
									index1Size[OBJECT.ordinal()], true, false);
							objectMapSequence1 = new SequenceLog64BigDisk(objectMap1DestinationPath,
									sharedNumbits2 + SECTION_SHIFT, index1Size[OBJECT.ordinal()], true, false);
							objectIdSequence2 = new SequenceLog64BigDisk(objectMap2OriginPath, objectNumbitsId2,
									index2Size[OBJECT.ordinal()], true, false);
							objectMapSequence2 = new SequenceLog64BigDisk(objectMap2DestinationPath,
									sharedNumbits1 + SECTION_SHIFT, index2Size[OBJECT.ordinal()], true, false);
						} else {
							subjectIdSequence1 = new SequenceLog64BigDisk(subjectMap1OriginPath, subjectNumbitsId1,
									subjectNSection1, true, true);
							subjectMapSequence1 = new SequenceLog64BigDisk(subjectMap1DestinationPath,
									sharedNumbits2 + SECTION_SHIFT, subjectNSection1, true, true);
							subjectIdSequence2 = new SequenceLog64BigDisk(subjectMap2OriginPath, subjectNumbitsId2,
									subjectNSection1, true, true);
							subjectMapSequence2 = new SequenceLog64BigDisk(subjectMap2DestinationPath,
									sharedNumbits1 + SECTION_SHIFT, subjectNSection1, true, true);

							objectIdSequence1 = new SequenceLog64BigDisk(objectMap1OriginPath, objectNumbitsId1,
									objectNSection1, true, true);
							objectMapSequence1 = new SequenceLog64BigDisk(objectMap1DestinationPath,
									sharedNumbits2 + SECTION_SHIFT, objectNSection1, true, true);
							objectIdSequence2 = new SequenceLog64BigDisk(objectMap2OriginPath, objectNumbitsId2,
									objectNSection1, true, true);
							objectMapSequence2 = new SequenceLog64BigDisk(objectMap2DestinationPath,
									sharedNumbits1 + SECTION_SHIFT, objectNSection1, true, true);

							Iterator<? extends CharSequence> itSubjectShared = dataset1.dataset().getDictionary()
									.stringIterator(SUBJECT, true);
							Iterator<? extends CharSequence> itObject = dataset1.dataset().getDictionary()
									.stringIterator(OBJECT, false);

							long maxNSection = Math.max(Math.max(subjectNSection1, subjectNSection2),
									Math.max(objectNSection1, objectNSection2)) << SECTION_SHIFT;

							try (QEPMapIdSorter subjectSorter = new QEPMapIdSorter(path.resolve("subjectIdSorter"),
									Math.max(subjectNSection1, objectNSection1), maxNSection);
									QEPMapIdSorter objectSorter = new QEPMapIdSorter(path.resolve("objectIdSorter"),
											Math.max(subjectNSection1, objectNSection1), maxNSection)) {
								Dictionary d2d = dataset2.dataset().getDictionary();
								long d2nshared = d2d.getNshared();

								long sequenceIndexSubject = 1;
								long componentIdCounterSubject = 1;

								while (itSubjectShared.hasNext()) {
									ByteString next = ByteString.of(itSubjectShared.next());
									long componentId = componentIdCounterSubject++;

									long mappedId = d2d.stringToId(next, SUBJECT);

									if (mappedId <= 0) {
										// can't find the id in the subjects,
										// searching in the objects
										mappedId = d2d.stringToId(next, OBJECT);

										if (mappedId <= d2nshared) {
											assert mappedId <= 0 : "found a mapped id";
											continue; // can't find the id in
											// both sections, ignore
										}
										// the id was find in the object
										// section, it can't be a shared element
										// we add the componentId -> mappedId
										// permutation
										subjectIdSequence1.set(sequenceIndexSubject, componentId);
										subjectMapSequence1.set(sequenceIndexSubject,
												((mappedId - d2nshared) << SECTION_SHIFT) | SECTION_TYPE_OBJECT);

										// we store the mappedId -> componentId
										// for future sorting
										objectSorter.addElement((mappedId - d2nshared),
												(componentId << SECTION_SHIFT) | SECTION_TYPE_SUBJECT);
									} else {
										// the id was find in the subject
										// section
										subjectIdSequence1.set(sequenceIndexSubject, componentId);
										subjectMapSequence1.set(sequenceIndexSubject,
												(mappedId << SECTION_SHIFT) | SECTION_TYPE_SUBJECT);

										subjectSorter.addElement(mappedId,
												(componentId << SECTION_SHIFT) | SECTION_TYPE_SUBJECT);
									}
									// assert componentId ==
									// dataset1.dataset().getDictionary().stringToId(next,
									// SUBJECT);

									sequenceIndexSubject++;
								}

								long sequenceIndexObject = 1;
								long componentIdCounterObject = 1;

								while (itObject.hasNext()) {
									ByteString next = ByteString.of(itObject.next());
									long componentId = componentIdCounterObject++;

									long mappedId;
									if (next.charAt(0) == '"') {
										// ignore literals
										mappedId = 0;
									} else {
										mappedId = d2d.stringToId(next, SUBJECT);
									}

									if (mappedId <= 0) {
										// can't find the id in the subjects,
										// searching in the objects
										mappedId = d2d.stringToId(next, OBJECT);

										if (mappedId <= d2nshared) {
											assert mappedId <= 0 : "found a shared mapped id";
											continue; // can't find the id in
											// both sections, ignore
										}
										objectIdSequence1.set(sequenceIndexObject, componentId);

										// we describe shared element as
										// subjects
										long oid = mappedId - d2nshared;

										objectMapSequence1.set(sequenceIndexObject,
												(oid << SECTION_SHIFT) | SECTION_TYPE_OBJECT);
										objectSorter.addElement(oid,
												(componentId << SECTION_SHIFT) | SECTION_TYPE_OBJECT);
									} else {
										// the id was find in the subject object

										// we add the componentId -> mappedId
										// permutation
										objectIdSequence1.set(sequenceIndexObject, componentId);
										objectMapSequence1.set(sequenceIndexObject,
												(mappedId << SECTION_SHIFT) | SECTION_TYPE_SUBJECT);

										// we store the mappedId -> componentId
										// for future sorting
										subjectSorter.addElement(mappedId,
												(componentId << SECTION_SHIFT) | SECTION_TYPE_OBJECT);
									}
									// assert componentId ==
									// dataset1.dataset().getDictionary().stringToId(next,
									// OBJECT) -
									// dataset1.dataset().getDictionary().getNshared();
									sequenceIndexObject++;
								}

								// we write the size in the header
								header.putLong(index1Location[SUBJECT.ordinal()], sequenceIndexSubject);
								header.putLong(index2Location[SUBJECT.ordinal()], subjectSorter.size() + 1);
								header.putLong(index1Location[OBJECT.ordinal()], sequenceIndexObject);
								header.putLong(index2Location[OBJECT.ordinal()], objectSorter.size() + 1);

								// we resize our files to reduce space usage
								subjectIdSequence1.resize(sequenceIndexSubject);
								subjectMapSequence1.resize(sequenceIndexSubject);
								subjectIdSequence2.resize(subjectSorter.size() + 1);
								subjectMapSequence2.resize(subjectSorter.size() + 1);

								objectIdSequence1.resize(sequenceIndexObject);
								objectMapSequence1.resize(sequenceIndexObject);
								objectIdSequence2.resize(objectSorter.size() + 1);
								objectMapSequence2.resize(objectSorter.size() + 1);

								// we can now sort the ids for the 2nd map
								subjectSorter.sort();
								objectSorter.sort();

								// we can write the sorted ids
								long sequenceIndex2 = 1;
								for (QEPMapIdSorter.QEPMapIds ids : subjectSorter) {
									subjectIdSequence2.set(sequenceIndex2, ids.origin());
									subjectMapSequence2.set(sequenceIndex2, ids.destination());
									sequenceIndex2++;
								}

								sequenceIndex2 = 1;
								for (QEPMapIdSorter.QEPMapIds ids : objectSorter) {
									objectIdSequence2.set(sequenceIndex2, ids.origin());
									objectMapSequence2.set(sequenceIndex2, ids.destination());
									sequenceIndex2++;
								}
							}
						}
						long nshared1 = dataset1.dataset().getDictionary().getNshared();
						long nshared2 = dataset2.dataset().getDictionary().getNshared();

						maps[SUBJECT.ordinal()] = new SectionMap(subjectIdSequence1, subjectMapSequence1,
								subjectIdSequence2, subjectMapSequence2, nshared1, nshared2);
						maps[OBJECT.ordinal()] = new SectionMap(objectIdSequence1, objectMapSequence1,
								objectIdSequence2, objectMapSequence2, nshared1, nshared2);
					} catch (Throwable t) {
						try {
							Closer.closeAll(subjectIdSequence1, subjectMapSequence1, subjectIdSequence2,
									subjectMapSequence2, objectIdSequence1, objectMapSequence1, objectIdSequence2,
									objectMapSequence2);
						} catch (Exception ee) {
							t.addSuppressed(ee);
						} catch (Error err) {
							err.addSuppressed(t);
							throw err;
						}
						throw t;
					}
				}

				// compute the converters for all the roles
				for (TripleComponentRole role : TripleComponentRole.valuesNoGraph()) {
					int roleId = role.ordinal();
					if (dataset1Base) {
						nodeConverters[roleId] = new DatasetNodeConverter(
								new PermutationNodeConverter(maps[roleId].idSequence1, maps[roleId].mapSequence1),
								new PermutationNodeConverter(maps[roleId].idSequence2, maps[roleId].mapSequence2));
					} else {
						// reverse the order because dataset1 isn't the same as
						// this.dataset1
						nodeConverters[roleId] = new DatasetNodeConverter(
								new PermutationNodeConverter(maps[roleId].idSequence2, maps[roleId].mapSequence2),
								new PermutationNodeConverter(maps[roleId].idSequence1, maps[roleId].mapSequence1));
					}
				}

				// add converter to only use the subject map for shared
				// components
				int subjectId = SUBJECT.ordinal();
				int objectId = OBJECT.ordinal();

				DatasetNodeConverter subjectConverters = nodeConverters[subjectId];
				DatasetNodeConverter objectConverters = nodeConverters[objectId];
				// add shared wrapper to switch to the subject converter with
				// shared elements
				nodeConverters[objectId] = new DatasetNodeConverter(
						new SharedWrapperNodeConverter(this.dataset1.dataset().getDictionary().getNshared(),
								subjectConverters.dataset1to2, objectConverters.dataset1to2),
						new SharedWrapperNodeConverter(this.dataset2.dataset().getDictionary().getNshared(),
								subjectConverters.dataset2to1, objectConverters.dataset2to1));

				// compute the CRC
				if (created) {
					crc.update(header, 0, HEADER_SIZE);
					crc.writeCRC(crcBuffer, 0);
				}
			}
			endSync.runAction(this);
		} catch (Throwable t) {
			try {
				close();
			} catch (Exception t2) {
				t.addSuppressed(t2);
			} catch (Error e) {
				e.addSuppressed(t);
				throw e;
			}
			throw t;
		}
	}

	/**
	 * sync old core version (it needs to be implemented after each core map
	 * change)
	 *
	 * @param version read core version
	 * @throws IOException sync exception
	 */
	public void syncOld(byte version) throws IOException {
		if (version > CORE_VERSION) {
			throw new IOException(
					format("read core map version %x, but the core is older (%x)", version, CORE_VERSION));
		}
		// handle old version convertion
		throw new IOException(format("read unknown core map version %x (current %x)", version, CORE_VERSION));
	}

	/**
	 * check if the permutations for a role should be regenerated, will check
	 * the file and throw error/warning if it is in an unstable state
	 *
	 * @param role    role
	 * @param created if the map was created
	 * @return if the permutation should be regenerated
	 * @throws IOException file state error
	 */
	private boolean checkRegen(TripleComponentRole role, boolean created) throws IOException {
		Path map1OriginPath = getMap1OriginPath(role);
		Path map1DestinationPath = getMap1DestinationPath(role);
		Path map2OriginPath = getMap2OriginPath(role);
		Path map2DestinationPath = getMap2DestinationPath(role);
		if (created) {
			checkNotExists(map1OriginPath);
			checkNotExists(map1DestinationPath);
			checkNotExists(map2OriginPath);
			checkNotExists(map2DestinationPath);
			return true;
		} else {
			boolean regen = !(Files.exists(map1OriginPath) && Files.exists(map1DestinationPath)
					&& Files.exists(map2OriginPath) && Files.exists(map2DestinationPath));
			if (regen) {
				boolean warn = false;
				if (Files.exists(map1OriginPath)) {
					logger.warn("{}: map1OriginPath[{}] is present, it will be removed! {}", this, role,
							map1OriginPath);
					warn = true;
				}
				if (Files.exists(map1DestinationPath)) {
					logger.warn("{}: map1DestinationPath[{}] is present, it will be removed! {}", this, role,
							map1DestinationPath);
					warn = true;
				}
				if (Files.exists(map2OriginPath)) {
					logger.warn("{}: map2OriginPath[{}] is present, it will be removed! {}", this, role,
							map2OriginPath);
					warn = true;
				}
				if (Files.exists(map2DestinationPath)) {
					logger.warn("{}: map2DestinationPath[{}] is present, it will be removed! {}", this, role,
							map2DestinationPath);
					warn = true;
				}
				if (warn) {
					logger.warn("{}: has a missing file from the grid", this);
				}
			}
			return regen;
		}
	}

	private void checkNotExists(Path path) throws IOException {
		if (Files.exists(path)) {
			throw new IOException("The file " + path + " already exists!");
		}
	}

	private void checkHeader(CloseMappedByteBuffer header, int shift, String id, QEPDataset dataset)
			throws IOException {
		Dictionary d = dataset.dataset().getDictionary();
		long nshared = d.getNshared();
		long nsubject = d.getNsubjects() - nshared;
		long npred = d.getNpredicates();
		long nobject = d.getNobjects() - nshared;

		long sh = header.getLong(shift);
		if (sh != nshared) {
			throw new IOException("Bad count for section SHARED for dataset header " + id + " " + sh + "!=" + nshared);
		}

		long s = header.getLong(shift + Long.BYTES);
		if (s != nsubject) {
			throw new IOException("Bad count for section SUBJECT for dataset header " + id + " " + s + "!=" + nsubject);
		}

		long p = header.getLong(shift + Long.BYTES * 2);
		if (p != npred) {
			throw new IOException("Bad count for section PREDICATE for dataset header " + id + " " + p + "!=" + npred);
		}

		long o = header.getLong(shift + Long.BYTES * 3);
		if (o != nobject) {
			throw new IOException("Bad count for section OBJECT for dataset header " + id + " " + o + "!=" + nobject);
		}
	}

	/**
	 * get a converter from a dataset to another
	 *
	 * @param dataset1 d1
	 * @param dataset2 d2
	 * @param role     role
	 * @return NodeConverter
	 * @throws AssertionError if dataset1 and dataset2 aren't represented by
	 *                        this QEPMap
	 */
	public NodeConverter getConverter(int dataset1, int dataset2, TripleComponentRole role) {
		if (this.dataset1.uid() == dataset1 && this.dataset2.uid() == dataset2) {
			return nodeConverters[role.ordinal()].dataset1to2;
		} else if (this.dataset1.uid() == dataset2 && this.dataset2.uid() == dataset1) {
			return nodeConverters[role.ordinal()].dataset2to1;
		} else {
			throw new AssertionError(
					"using bad ids to fetch converter " + dataset1 + "/" + dataset2 + " with map " + this);
		}
	}

	QEPDataset getDataset(int uid) {
		if (this.dataset1.uid() == uid) {
			return this.dataset1;
		}
		if (this.dataset2.uid() == uid) {
			return this.dataset2;
		}
		throw new IllegalArgumentException(format("bad uid: %d in %s context", uid, getUid()));
	}

	/**
	 * @return Map id
	 */
	public String getMapId() {
		return this.dataset1.id() + "-" + this.dataset2.id();
	}

	/**
	 * @return UID only valid as long as the same QEPCore is used
	 */
	public Uid getUid() {
		return uid;
	}

	/**
	 * @return the map path of this linker object
	 */
	public Path getMapHeaderPath() {
		return path.resolve("header.qepm");
	}

	/**
	 * @return the sequence location of the origin part of the map 1
	 */
	public Path getMap1OriginPath(TripleComponentRole role) {
		return path.resolve(role.getTitle() + ".map1.ori.seq");
	}

	/**
	 * @return the sequence location of the origin part of the map 2
	 */
	public Path getMap2OriginPath(TripleComponentRole role) {
		return path.resolve(role.getTitle() + ".map2.orig.seq");
	}

	/**
	 * @return the sequence location of the destination part of the map 1
	 */
	public Path getMap1DestinationPath(TripleComponentRole role) {
		return path.resolve(role.getTitle() + ".map1.dest.seq");
	}

	/**
	 * @return the sequence location of the destination part of the map 2
	 */
	public Path getMap2DestinationPath(TripleComponentRole role) {
		return path.resolve(role.getTitle() + ".map2.dest.seq");
	}

	/**
	 * say if the dataset 1 is the dataset1 is using a direct map for a
	 * particular role
	 *
	 * @return true if the dataset 1 is used
	 */
	public boolean isMapDataset1Smaller() {
		return useDataset1;
	}

	/**
	 * delete all the associate links
	 *
	 * @throws IOException io exception
	 */
	public void deleteLink() throws IOException {
		List<Closeable> paths = new ArrayList<>();

		paths.add(CloseSuppressPath.of(getMapHeaderPath()));

		for (TripleComponentRole role : TripleComponentRole.valuesNoGraph()) {
			paths.add(CloseSuppressPath.of(getMap1DestinationPath(role)));
			paths.add(CloseSuppressPath.of(getMap1OriginPath(role)));
			paths.add(CloseSuppressPath.of(getMap2DestinationPath(role)));
			paths.add(CloseSuppressPath.of(getMap2OriginPath(role)));
		}

		Closer.closeAll(paths);

		try {
			Files.deleteIfExists(path);
		} catch (IOException ignore) {
			// we ignore it because maybe the directory can contain junk from
			// the user
		}
	}

	@Override
	public String toString() {
		return "QEPMap[" + dataset1 + "->" + dataset2 + ", path=" + path + "]";
	}

	@Override
	public void close() throws IOException {
		try {
			Closer.closeSingle(maps);
		} finally {
			Arrays.fill(maps, null);
		}
	}
}

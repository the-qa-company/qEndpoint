package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap375Big;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.storage.converter.DirectNodeConverter;
import com.the_qa_company.qendpoint.core.storage.converter.NodeConverter;
import com.the_qa_company.qendpoint.core.storage.converter.SelectNodeConverter;
import com.the_qa_company.qendpoint.core.storage.converter.SharedWrapperNodeConverter;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;

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

import static com.the_qa_company.qendpoint.core.enums.TripleComponentRole.OBJECT;
import static com.the_qa_company.qendpoint.core.enums.TripleComponentRole.SUBJECT;

/**
 * Linker to fetch data from a dataset to another one
 *
 * @author Antoine Willerval
 */
public class QEPMap implements Closeable {
	public static final long SECTION_MAST = 1;
	public static final long SECTION_SHIFT = 1;
	public static final long SECTION_TYPE_SUBJECT = 0;
	public static final long SECTION_TYPE_OBJECT = 1;
	private static final long HEADER_SIZE;
	private static final byte[] MAGIC = "$QML".getBytes(ByteStringUtil.STRING_ENCODING);

	static {
		long headerSize = 0;

		// magic: byte[MAGIC.length]
		headerSize += MAGIC.length * Byte.BYTES;
		// id sizes: (ID size + 1) * 2 (+1 for '\0')
		headerSize += (QEPCore.MAX_ID_SIZE + 1) * Byte.BYTES * 2;
		// 4 sections * 2 datasets
		headerSize += Long.BYTES * 4 * 2;

		// select section sizes
		headerSize += (long) Long.BYTES * TripleComponentRole.values().length;

		HEADER_SIZE = headerSize;
	}

	private record SectionMap(ModifiableBitmap selectBitmap, DynamicSequence selectSequence,
	                          DynamicSequence directSequence) {
	}

	private record DatasetNodeConverter(NodeConverter dataset1to2, NodeConverter dataset2to1) {
	}

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
	public static long getIdOfMapped(long mappedId) {
		return mappedId >>> SECTION_SHIFT;
	}

	private final QEPDataset dataset1;
	private final QEPDataset dataset2;

	private final Path path;
	private final Uid uid;
	private final SectionMap[] maps = new SectionMap[TripleComponentRole.values().length];
	private final boolean[] useDataset1 = new boolean[maps.length];
	private final DatasetNodeConverter[] nodeConverters = new DatasetNodeConverter[maps.length];

	QEPMap(Path parent, QEPDataset dataset1, QEPDataset dataset2) {
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

		// compute the useDataset1 array
		for (TripleComponentRole role : TripleComponentRole.values()) {
			int roleid = role.ordinal();
			boolean direct = isMapDataset1Direct0(role);
			useDataset1[roleid] = direct;
		}
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
			// create the sync path
			Files.createDirectories(path);
			boolean created = !Files.exists(mapHeaderPath);
			// race condition
			// we open the header file to see if it's actually the right map
			try (
					FileChannel channel = FileChannel.open(
							mapHeaderPath,
							StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE
					);
					CloseMappedByteBuffer header = IOUtil.mapChannel(
							mapHeaderPath, channel,
							FileChannel.MapMode.READ_WRITE,
							0, HEADER_SIZE
					)
			) {
				// if the file is empty, it means it was created (I hope)
				long[] indexSelectSize = new long[TripleComponentRole.values().length];
				int[] indexSelectLocation = new int[indexSelectSize.length];

				int shift = 0;

				if (created) {
					// we write the header
					// write magic
					for (; shift < MAGIC.length; shift++) {
						header.put(shift, MAGIC[shift]);
					}
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

					for (int i = 0; i < indexSelectLocation.length; i++) {
						indexSelectLocation[i] = shift;
						shift += Long.BYTES;
					}

					assert shift == HEADER_SIZE : shift + "!=" + HEADER_SIZE;
				} else {
					// we check the magic
					for (; shift < MAGIC.length; shift++) {
						byte cc = header.get(shift);
						if (cc != MAGIC[shift]) {
							throw new IOException("Can't read magic number of dataset linker " + getMapId() + "!");
						}
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

					for (int i = 0; i < indexSelectSize.length; i++) {
						indexSelectSize[i] = header.getLong(shift);
						shift += Long.BYTES;
					}

					assert shift == HEADER_SIZE : shift + "!=" + HEADER_SIZE;
				}

				// load the sections
				for (TripleComponentRole role : TripleComponentRole.values()) {
					int roleId = role.ordinal();
					Path directSequencePath = getMapDirectPath(role);
					Path selectBitmapPath = getMapSelectBitmapPath(role);
					Path selectSequencePath = getMapSelectSequencePath(role);

					// check that the files aren't existing (race conditions...)
					boolean regenRole;
					if (created) {
						checkNotExists(directSequencePath);
						checkNotExists(selectBitmapPath);
						checkNotExists(selectSequencePath);
						regenRole = true;
					} else {
						regenRole = !Files.exists(directSequencePath) || !Files.exists(selectBitmapPath) ||
								!Files.exists(selectSequencePath);
					}

					Bitmap375Big selectBitmap = null;
					DynamicSequence directSequence = null;
					DynamicSequence selectSequence = null;

					try {
						// size of the small dictionary
						long directSize = directMapSize(role);
						// size of the big dictionary
						long selectSize = selectBitmapSize(role);
						boolean predicateRole = role == TripleComponentRole.PREDICATE;
						if (!regenRole) {
							selectBitmap = Bitmap375Big.map(selectBitmapPath, selectSize, true);
							selectSequence = new SequenceLog64BigDisk(
									selectSequencePath,
									(predicateRole ? 0 : 1) + BitUtil.log2(directMaxMapSize(role)),
									indexSelectSize[roleId] + 1,
									true, false
							);
							directSequence = new SequenceLog64BigDisk(
									directSequencePath,
									(predicateRole ? 0 : 1) + BitUtil.log2(selectMaxMapSize(role)),
									directSize + 1,
									true, false
							);
						} else {
							selectBitmap = Bitmap375Big.disk(selectBitmapPath, selectSize, true);
							// we use direct size because we know that the size can't be higher than the small dictionary
							// we overwrite the direct to set everything to 0 because we might have 0
							// we don't overwrite the select because the 0s will be trimmed
							selectSequence = new SequenceLog64BigDisk(
									selectSequencePath,
									(predicateRole ? 0 : 1) + BitUtil.log2(directMaxMapSize(role)),
									directSize + 1,
									true, false
							);
							directSequence = new SequenceLog64BigDisk(
									directSequencePath,
									(predicateRole ? 0 : 1) + BitUtil.log2(selectMaxMapSize(role)),
									directSize + 1,
									true, true
							);

							// generate the index
							QEPDataset dsDirect;
							QEPDataset dsSelect;

							if (isMapDataset1Direct(role)) {
								dsDirect = this.dataset1;
								dsSelect = this.dataset2;
							} else {
								dsDirect = this.dataset2;
								dsSelect = this.dataset1;
							}

							Iterator<? extends CharSequence> components = dsDirect.dataset().getDictionary()
									.stringIterator(role, role == SUBJECT);

							try (QEPMapIdSorter sorter = new QEPMapIdSorter(path.resolve("sorter"), directSize, Math.max(directSize, selectSize))) {
								long directIndex = 1;
								long componentIndex = 1;

								while (components.hasNext()) {
									CharSequence next = components.next();
									Dictionary dictionary = dsSelect.dataset().getDictionary();
									long mappedIndex = dictionary.stringToId(next, role);

									if (mappedIndex > 0) {
										if (predicateRole) {
											// we map the [componentIndex] -> mappedIndex
											directSequence.set(componentIndex, mappedIndex);
											// we map the [mappedIndex]
											sorter.addElement(mappedIndex, directIndex);
										} else {
											long type = role == OBJECT ? SECTION_TYPE_OBJECT : SECTION_TYPE_SUBJECT;

											// we map the [componentIndex] -> mappedIndex
											directSequence.set(componentIndex, (mappedIndex << SECTION_SHIFT) | type);
											// we map the [mappedIndex]
											sorter.addElement(mappedIndex, (directIndex << SECTION_SHIFT) | type);
										}
									} else if (!predicateRole) {
										// we search in the opposite section
										TripleComponentRole other = role == OBJECT ? SUBJECT : OBJECT;
										long mappedIndex2 = dictionary.stringToId(next, other);

										if (mappedIndex2 > 0) {
											long type = other == OBJECT ? SECTION_TYPE_OBJECT : SECTION_TYPE_SUBJECT;

											// we map the [componentIndex] -> mappedIndex
											directSequence.set(componentIndex, (mappedIndex2 << SECTION_SHIFT) | type);
											// we map the [mappedIndex]
											sorter.addElement(mappedIndex2, (directIndex << SECTION_SHIFT) | type);
										}
									}

									componentIndex++;
									directIndex++;
								}

								// we need to sort due to cross dictionary type/section read order
								sorter.sort();

								long selectIndex = 0;

								// we read the sorted elements and using the selectIndex to avoid useless computing
								for (long i = 0; i < sorter.size(); i++) {
									QEPMapIdSorter.QEPMapIds id = sorter.get(i);

									selectBitmap.set(id.origin(), true);
									selectSequence.set(selectIndex++, id.destination());
								}
							}

							// no aggressive trimming because it's most likely useless (maybe 1 or 2 bits)
							selectSequence.trimToSize();
							// we set the size in the header
							header.putLong(indexSelectLocation[roleId], selectSize);
						}
					} catch (Throwable t) {
						try {
							Closer.closeAll(selectBitmap, selectSequence, directSequence);
						} catch (Exception e) {
							t.addSuppressed(e);
						} catch (Error err) {
							err.addSuppressed(t);
							throw err;
						}
						throw t;
					}

					maps[roleId] = new SectionMap(selectBitmap, selectSequence, directSequence);
				}
				for (TripleComponentRole role : TripleComponentRole.values()) {
					int roleId = role.ordinal();
					SectionMap map = maps[roleId];

					DirectNodeConverter directNodeConverter = new DirectNodeConverter(map.directSequence);
					SelectNodeConverter selectNodeConverter = new SelectNodeConverter(map.selectBitmap, map.selectSequence);

					if (isMapDataset1Direct(role)) {
						nodeConverters[roleId] = new DatasetNodeConverter(
								directNodeConverter,
								selectNodeConverter
						);
					} else {
						nodeConverters[roleId] = new DatasetNodeConverter(
								selectNodeConverter,
								directNodeConverter
						);
					}
				}

				int subjectId = SUBJECT.ordinal();
				int objectId = OBJECT.ordinal();
				DatasetNodeConverter subjectConverters = nodeConverters[subjectId];
				DatasetNodeConverter objectConverters = nodeConverters[objectId];
				// add shared wrapper to switch to the subject converter with shared elements
				nodeConverters[objectId] = new DatasetNodeConverter(
						new SharedWrapperNodeConverter(
								dataset1.dataset().getDictionary().getNshared(),
								subjectConverters.dataset1to2, objectConverters.dataset1to2
						),
						new SharedWrapperNodeConverter(
								dataset2.dataset().getDictionary().getNshared(),
								subjectConverters.dataset2to1, objectConverters.dataset2to1
						)
				);
			}
		} catch (
				Throwable t) {
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

	private void checkNotExists(Path path) throws IOException {
		if (Files.exists(path)) {
			throw new IOException("The file " + path + " already exists!");
		}
	}

	private void checkHeader(CloseMappedByteBuffer header, int shift, String id, QEPDataset dataset) throws IOException {
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

	public NodeConverter getConverter(int dataset1, int dataset2, TripleComponentRole role) {
		if (this.dataset1.uid() == dataset1 && this.dataset2.uid() == dataset2) {
			return nodeConverters[role.ordinal()].dataset1to2;
		} else if (this.dataset1.uid() == dataset2 && this.dataset2.uid() == dataset1) {
			return nodeConverters[role.ordinal()].dataset2to1;
		} else {
			throw new AssertionError("using bad ids to fetch converter " + dataset1 + "/" + dataset2 + " with dataset " + this.dataset1.uid() + "/" + this.dataset2.uid());
		}
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
	 * @return the sequence location of the direct map
	 */
	public Path getMapDirectPath(TripleComponentRole role) {
		return path.resolve(role.getTitle() + ".direct.seq");
	}

	/**
	 * @return the bitmap location of the select map
	 */
	public Path getMapSelectBitmapPath(TripleComponentRole role) {
		return path.resolve(role.getTitle() + ".select.bin");
	}

	/**
	 * @return the sequence location of the select map
	 */
	public Path getMapSelectSequencePath(TripleComponentRole role) {
		return path.resolve(role.getTitle() + ".select.seq");
	}

	private long getDatasetSize(QEPDataset dataset, TripleComponentRole role) {
		switch (role) {
			case SUBJECT -> {
				return dataset.dataset().getDictionary().getNsubjects();
			}
			case PREDICATE -> {
				return dataset.dataset().getDictionary().getNpredicates();
			}
			case OBJECT -> {
				final Dictionary d = dataset.dataset().getDictionary();
				return d.getNobjects() - d.getNshared();
			}
			default -> throw new IllegalFormatException("bad triple role: " + role);
		}
	}

	private long getMapMaxDatasetSize(QEPDataset dataset, TripleComponentRole role) {
		switch (role) {
			case SUBJECT, OBJECT -> {
				return Math.max(getDatasetSize(dataset, SUBJECT), getDatasetSize(dataset, OBJECT));
			}
			case PREDICATE -> {
				return getDatasetSize(dataset, role);
			}
			default -> throw new IllegalFormatException("bad triple role: " + role);
		}
	}

	private long directMapSize(TripleComponentRole role) {
		if (isMapDataset1Direct(role)) {
			return getDatasetSize(dataset1, role);
		} else {
			return getDatasetSize(dataset2, role);
		}
	}

	private long selectBitmapSize(TripleComponentRole role) {
		if (!isMapDataset1Direct(role)) {
			return getDatasetSize(dataset1, role);
		} else {
			return getDatasetSize(dataset2, role);
		}
	}

	private long directMaxMapSize(TripleComponentRole role) {
		if (isMapDataset1Direct(role)) {
			return getMapMaxDatasetSize(dataset1, role);
		} else {
			return getMapMaxDatasetSize(dataset2, role);
		}
	}

	private long selectMaxMapSize(TripleComponentRole role) {
		if (!isMapDataset1Direct(role)) {
			return getMapMaxDatasetSize(dataset1, role);
		} else {
			return getMapMaxDatasetSize(dataset2, role);
		}
	}

	/**
	 * say if the dataset 1 is the dataset1 is using a direct map for a particular role
	 *
	 * @param role the role
	 * @return true if the dataset 1 is used
	 */
	private boolean isMapDataset1Direct0(TripleComponentRole role) {
		return getDatasetSize(dataset1, role) < getDatasetSize(dataset2, role);
	}

	/**
	 * say if the dataset 1 is the dataset1 is using a direct map for a particular role
	 *
	 * @param role the role
	 * @return true if the dataset 1 is used
	 */
	public boolean isMapDataset1Direct(TripleComponentRole role) {
		return useDataset1[role.ordinal()];
	}

	/**
	 * delete all the associate links
	 *
	 * @throws IOException io exception
	 */
	public void deleteLink() throws IOException {
		List<Closeable> paths = new ArrayList<>();

		paths.add(() -> Files.deleteIfExists(getMapHeaderPath()));

		for (TripleComponentRole role : TripleComponentRole.values()) {
			paths.add(() -> Files.deleteIfExists(getMapDirectPath(role)));
			paths.add(() -> Files.deleteIfExists(getMapSelectBitmapPath(role)));
			paths.add(() -> Files.deleteIfExists(getMapSelectSequencePath(role)));
		}

		paths.add(() -> Files.deleteIfExists(path));

		Closer.closeAll(paths);
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

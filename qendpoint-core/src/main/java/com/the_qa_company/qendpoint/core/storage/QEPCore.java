package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap64Big;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.iterator.utils.CatIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.storage.converter.NodeConverter;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.ContainerException;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_KEY;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_LOCATION;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_SUBINDEX;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.DICTIONARY_TYPE_KEY;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.HDTCAT_DELETE_LOCATION;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.HDTCAT_FUTURE_LOCATION;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.HDTCAT_LOCATION;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.LOADER_CATTREE_KCAT;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.LOADER_CATTREE_LOADERTYPE_KEY;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.LOADER_CATTREE_LOCATION_KEY;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.LOADER_CATTREE_MEMORY_FAULT_FACTOR;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.LOADER_DISK_LOCATION_KEY;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.LOADER_TYPE_KEY;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.LOADER_TYPE_VALUE_CAT;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.LOADER_TYPE_VALUE_DISK;

/**
 * qEndpoint's core
 *
 * @author Antoine Willerval
 */
public class QEPCore implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(QEPCore.class);
	/**
	 * the max size of a dataset id
	 */
	public static final int MAX_ID_SIZE = 100;
	/**
	 * regex to check id file
	 */
	public static final Pattern ID_REGEX = Pattern.compile("[0-9a-z_\\-]{1," + MAX_ID_SIZE + "}");
	/**
	 * load the dataset into memory
	 */
	public static final String OPTION_IN_MEMORY_DATASET = "qep.dataset.load";
	/**
	 * do not load dataset except the SPO dataset
	 */
	public static final String OPTION_NO_CO_INDEX = "qep.dataset.no_co_index";
	/**
	 * prefix of the datasets in the {@link #FILE_DATASET_STORE} directory
	 */
	public static final String FILE_DATASET_PREFIX = "index_";
	/**
	 * suffix of the datasets in the {@link #FILE_DATASET_STORE} directory
	 */
	public static final String FILE_DATASET_SUFFIX = ".hdt";
	/**
	 * directory where the datasets are
	 */
	public static final String FILE_DATASET_STORE = "store";
	/**
	 * directory where the maps are
	 */
	public static final String FILE_DATASET_MAPS = "maps";
	/**
	 * config file of the core
	 */
	public static final String FILE_CORE_CONFIG_OPT = "config.opt";

	private final Map<String, QEPDataset> dataset = new HashMap<>();
	private final Map<Integer, QEPDataset> datasetByUid = new HashMap<>();
	private final Map<Uid, QEPMap> map = new HashMap<>();

	// config
	private final HDTOptions options;
	private final boolean memoryDataset;
	private final boolean noCoIndex;
	private final Path location;

	/**
	 * QEP core
	 *
	 * @param location       location of the core
	 * @param defaultOptions default options
	 * @throws QEPCoreException can't init the core
	 */
	public QEPCore(Path location, HDTOptions defaultOptions) throws QEPCoreException {
		this(location, defaultOptions, false);
	}

	/**
	 * QEP core
	 *
	 * @param location         location of the core
	 * @param defaultOptions   default options
	 * @param overwriteOptions overwrite the options from the previous core
	 * @throws QEPCoreException can't init the core
	 */
	public QEPCore(Path location, HDTOptions defaultOptions, boolean overwriteOptions) throws QEPCoreException {
		this.location = Objects.requireNonNull(location);

		HDTOptions opt;
		Path optionsPath = getOptionsPath();

		boolean optExists = Files.exists(optionsPath);
		// load the option file if it exists, it's better to keep
		if (!overwriteOptions && optExists) {
			try {
				opt = HDTOptions.readFromFile(optionsPath);
			} catch (IOException e) {
				throw new QEPCoreException("Can't read options", e);
			}
		} else {
			opt = HDTOptions.ofNullable(defaultOptions);
		}

		// copy the options in a push-bottom way to set default options
		this.options = opt.pushBottom();

		this.options.setOptions(
				// set our dictionary type
				DICTIONARY_TYPE_KEY, DICTIONARY_TYPE_VALUE_MULTI_OBJECTS,

				// set the disk indexing
				BITMAPTRIPLES_INDEX_METHOD_KEY, BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK,
				// with the disk sequence
				BITMAPTRIPLES_SEQUENCE_DISK, true,
				// sub index
				BITMAPTRIPLES_SEQUENCE_DISK_SUBINDEX, true,
				// indexing work location
				BITMAPTRIPLES_SEQUENCE_DISK_LOCATION, location.resolve("work"),

				// generation using cat
				LOADER_TYPE_KEY, LOADER_TYPE_VALUE_CAT,
				// gen location
				LOADER_DISK_LOCATION_KEY, location.resolve("gen"),
				// gen temp dataset
				LOADER_DISK_FUTURE_HDT_LOCATION_KEY, location.resolve("gen-wip.hdt"),

				LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY, location.resolve("cat-wip.hdt"),
				LOADER_CATTREE_LOADERTYPE_KEY, LOADER_TYPE_VALUE_DISK,
				LOADER_CATTREE_LOCATION_KEY, location.resolve("cattree"),
				LOADER_CATTREE_MEMORY_FAULT_FACTOR, 1,
				LOADER_CATTREE_KCAT, 20,

				HDTCAT_DELETE_LOCATION, true,
				HDTCAT_LOCATION, location.resolve("hdtcat"),
				HDTCAT_FUTURE_LOCATION, location.resolve("catgen.hdt")
		);

		if (!optExists || overwriteOptions) {
			try {
				// write the options file
				Files.createDirectories(optionsPath.getParent());
				this.options.write(optionsPath);
			} catch (IOException e) {
				logger.warn("Can't write option file", e);
			}
		}

		memoryDataset = this.options.getBoolean(OPTION_IN_MEMORY_DATASET, false);
		noCoIndex = this.options.getBoolean(OPTION_NO_CO_INDEX, false);

		try {
			// load the dataset and sync the maps
			reloadDataset();
			syncDatasetMaps();
		} catch (Throwable t) {
			try {
				close();
			} catch (Throwable t2) {
				t.addSuppressed(t2);
			}
			throw t;
		}
	}

	/**
	 * @return the number of triples
	 */
	public long triplesCount() {
		return dataset.values().stream()
				.mapToLong(d -> d.dataset().getTriples().getNumberOfElements())
				.sum();
	}

	@SuppressWarnings("resource")
	private QEPDataset openDataset(String id, Path path) throws IOException {
		HDT dataset = null;
		ModifiableBitmap bitmap = null;
		ModifiableBitmap[] deltaBitmaps = new ModifiableBitmap[TripleComponentRole.values().length];
		try {
			if (memoryDataset) {
				if (noCoIndex) {
					dataset = HDTManager.loadHDT(path, ProgressListener.ignore(), options);
				} else {
					dataset = HDTManager.loadIndexedHDT(path, ProgressListener.ignore(), options);
				}
			} else {
				if (noCoIndex) {
					dataset = HDTManager.mapHDT(path, ProgressListener.ignore(), options);
				} else {
					dataset = HDTManager.mapIndexedHDT(path, options, ProgressListener.ignore());
				}
			}

			Path deleteBitmapPath = path.resolveSibling(path.getFileName() + ".delete.bm");

			if (Files.exists(deleteBitmapPath)) {
				bitmap = Bitmap64Big.map(deleteBitmapPath, dataset.getTriples().getNumberOfElements());
			} else {
				bitmap = Bitmap64Big.disk(deleteBitmapPath, dataset.getTriples().getNumberOfElements());
			}

			for (TripleComponentRole role : TripleComponentRole.values()) {
				Path deltaBitmapPath = path.resolveSibling(path.getFileName() + ".delta-" + role.getTitle() + ".bm");
				long size = dataset.getDictionary().getNSection(role, role == TripleComponentRole.SUBJECT);

				if (Files.exists(deltaBitmapPath)) {
					deltaBitmaps[role.ordinal()] = Bitmap64Big.map(deltaBitmapPath, size);
				} else {
					deltaBitmaps[role.ordinal()] = Bitmap64Big.disk(deltaBitmapPath, size);
				}
			}

			return new QEPDataset(this, id, path, dataset, bitmap, deltaBitmaps);
		} catch (Throwable t) {
			Closer.closeAll(t, dataset, bitmap, deltaBitmaps);
			throw new AssertionError();
		}
	}

	/**
	 * Reload the dataset with the disk
	 *
	 * @throws QEPCoreException reload issue
	 */
	public void reloadDataset() throws QEPCoreException {
		try {
			// clear the pool
			Closer.closeAll(dataset);
			dataset.clear();
			datasetByUid.clear();
			try (Stream<Path> files = Files.list(getDatasetPath())) {
				files.forEach(path -> {
					String filename = path.getFileName().toString();

					if (!filename.startsWith(FILE_DATASET_PREFIX) || !filename.endsWith(FILE_DATASET_SUFFIX)) {
						// we ignore this file because it's most likely not what we're searching for
						return;
					}

					String indexId = filename.substring(FILE_DATASET_PREFIX.length(), filename.length() - FILE_DATASET_SUFFIX.length()).toLowerCase();


					Matcher matcher = ID_REGEX.matcher(indexId);
					if (!matcher.matches()) {
						logger.warn("file {} seems to be a dataset, but isn't matching the id format, it will be ignored.", path);
						return;
					}
					// load the dataset
					try {
						QEPDataset ds = openDataset(indexId, path);
						QEPDataset other = dataset.put(ds.id(), ds);

						// Windows compatibility, it would also be a bad practice to use datasets
						// with case-sensitive names
						if (other != null) {
							ContainerException e = new ContainerException(
									new QEPCoreException("Dataset collision with name " + other.id() + " at path " + other.path() + " vs " + ds.path())
							);
							try {
								// close the other dataset
								other.close();
							} catch (Exception t) {
								// issue while closing
								e.addSuppressed(t);
							} catch (Error t) {
								t.addSuppressed(e);
								throw t;
							}
							throw e;
						}
						datasetByUid.put(ds.uid(), ds);
					} catch (IOException e) {
						throw new ContainerException(new QEPCoreException(e));
					}
				});
			} catch (Throwable t) {
				// close all the previously loaded dateset
				try {
					Closer.closeAll(dataset);
				} catch (Throwable t2) {
					t.addSuppressed(t2);
				}
				throw t;
			}
		} catch (IOException e) {
			throw new QEPCoreException(e);
		} catch (ContainerException e) {
			if (e.getCause() instanceof QEPCoreException ee) {
				throw ee;
			}
			throw new QEPCoreException(e.getCause());
		}
	}

	/**
	 * sync all the maps of the dataset
	 *
	 * @throws QEPCoreException sync exception
	 */
	public void syncDatasetMaps() throws QEPCoreException {
		Path mapsDir = getMapsPath();
		try {
			// close the previous maps
			Closer.closeAll(map.values());
			map.clear();
			Files.createDirectories(mapsDir);

			for (QEPDataset d1 : dataset.values()) {
				for (QEPDataset d2 : dataset.values()) {
					bindDataset(d1, d2);
				}
			}
		} catch (IOException e) {
			throw new QEPCoreException("Can't sync map data!", e);
		}
	}

	/**
	 * get a node converter from 2 UIDs and a role
	 *
	 * @param originUID      origin UID
	 * @param destinationUID destination UID
	 * @param role           role
	 * @return converter
	 * @throws QEPCoreException can't find the map in the core
	 */
	public NodeConverter getConverter(int originUID, int destinationUID, TripleComponentRole role) throws QEPCoreException {
		Uid uid = Uid.of(originUID, destinationUID);

		QEPMap qepMap = map.get(uid);
		if (qepMap == null) {
			throw new QEPCoreException("Can't find map with UID " + uid);
		}

		return qepMap.getConverter(originUID, destinationUID, role);
	}

	/**
	 * get a mapping between 2 datasets
	 *
	 * @param uid uid of the 2 datasets
	 * @return mapping, or null if unavailable, bad uid or unsync maps?
	 */
	public QEPMap getMappingById(Uid uid) {
		return map.get(uid);
	}

	/**
	 * @return all the dataset UIDs
	 */
	public Set<Integer> getDatasetUids() {
		return Collections.unmodifiableSet(datasetByUid.keySet());
	}

	/**
	 * @return all the dataset IDs
	 */
	public Set<String> getDatasetIds() {
		return Collections.unmodifiableSet(dataset.keySet());
	}

	/**
	 * @return the datasets
	 */
	public Collection<QEPDataset> getDatasets() {
		return Collections.unmodifiableCollection(dataset.values());
	}

	/**
	 * bind 2 datasets in the core
	 *
	 * @param dataset1 dataset 1
	 * @param dataset2 dataset 2
	 * @throws IOException bind exception
	 */
	private void bindDataset(QEPDataset dataset1, QEPDataset dataset2) throws IOException {
		if (dataset1.uid() == dataset2.uid()) {
			return; // same object
		}

		if (dataset1.id().equals(dataset2.id())) {
			return; // same id
		}

		// create a map for our 2 datasets
		QEPMap qepMap = new QEPMap(getMapsPath(), dataset1, dataset2);

		// try to add this map to our maps
		QEPMap old = map.putIfAbsent(qepMap.getUid(), qepMap);

		if (old == null) {
			// it was added, meaning we need to sync it
			qepMap.sync();
		}
	}

	/**
	 * search a triple into the core
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public Iterator<? extends QEPComponentTriple> search(CharSequence subject, CharSequence predicate, CharSequence object) throws QEPCoreException {
		return search(
				createComponentByString(subject),
				createComponentByString(predicate),
				createComponentByString(object)
		);
	}

	/**
	 * search a triple into the core
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public Iterator<? extends QEPComponentTriple> search(QEPComponent subject, QEPComponent predicate, QEPComponent object) throws QEPCoreException {
		return search(QEPComponentTriple.of(subject, predicate, object));
	}

	/**
	 * search a triple into the core
	 *
	 * @param triple triple to search
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public Iterator<? extends QEPComponentTriple> search(TripleString triple) throws QEPCoreException {
		return search(
				triple.getSubject().isEmpty() ? null : triple.getSubject(),
				triple.getPredicate().isEmpty() ? null : triple.getPredicate(),
				triple.getObject().isEmpty() ? null : triple.getObject()
		);
	}

	/**
	 * search a triple into the core
	 *
	 * @param triple triple to search
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public Iterator<? extends QEPComponentTriple> search(QEPComponentTriple triple) throws QEPCoreException {
		List<Iterator<QEPComponentTriple>> iterators = new ArrayList<>();
		QEPComponentTriple clone = triple.freeze();

		for (QEPDataset value : dataset.values()) {
			iterators.add(value.search(clone));
		}

		// cat all the iterators
		return CatIterator.of(iterators);
	}

	/**
	 * @return the dataset path
	 */
	public Path getDatasetPath() {
		return location.resolve(FILE_DATASET_STORE);
	}

	/**
	 * @return the maps path
	 */
	public Path getMapsPath() {
		return location.resolve(FILE_DATASET_MAPS);
	}

	/**
	 * @return the options file path
	 */
	public Path getOptionsPath() {
		return location.resolve(FILE_CORE_CONFIG_OPT);
	}

	/**
	 * @return an unmodifiable {@link HDTOptions} of the core's options
	 */
	public HDTOptions getOptions() {
		return options.readOnly();
	}

	@Override
	public void close() throws QEPCoreException {
		try {
			Closer.closeAll(dataset);
		} catch (IOException e) {
			throw new QEPCoreException(e);
		}
	}

	/**
	 * get a dataset by its id
	 *
	 * @param id the dataset id
	 * @return dataset, or null if undefined in this core
	 */
	public QEPDataset getDatasetById(String id) {
		return dataset.get(id);
	}

	/**
	 * get a dataset by its uid
	 *
	 * @param uid the dataset uid
	 * @return dataset, or null if undefined in this core
	 */
	public QEPDataset getDatasetByUid(int uid) {
		return datasetByUid.get(uid);
	}

	/**
	 * create a component from a CharSequence
	 *
	 * @param seq the sequence
	 * @return QEPComponent
	 */
	public QEPComponent createComponentByString(CharSequence seq) {
		if (seq == null || seq.isEmpty()) {
			return null;
		}
		// convert it to avoid overhead in dict methods
		ByteString bs = ByteString.of(seq);
		return new QEPComponent(this, null, null, 0, bs);
	}

	/**
	 * create a component using its id in a dataset
	 *
	 * @param dataset the dataset
	 * @param id      the id in the dataset
	 * @param role    the role in the dataset
	 * @return QEPComponent
	 */
	public QEPComponent createComponentById(QEPDataset dataset, long id, DictionarySectionRole role) {
		return dataset.component(id, role);
	}

	/**
	 * create a component using its id in a dataset
	 *
	 * @param dataset the dataset
	 * @param id      the id in the dataset
	 * @param role    the role in the dataset
	 * @return QEPComponent
	 */
	public QEPComponent createComponentById(QEPDataset dataset, long id, TripleComponentRole role) {
		return dataset.component(id, role);
	}
}

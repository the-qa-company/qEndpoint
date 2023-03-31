package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.ContainerException;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
	/**
	 * the max size of a dataset id
	 */
	public static final int MAX_ID_SIZE = 100;
	/**
	 * regex to check id file
	 */
	public static final Pattern ID_REGEX = Pattern.compile("[0-9a-z_\\-]{1," + MAX_ID_SIZE + "}");
	private static final Logger logger = LoggerFactory.getLogger(QEPCore.class);
	/**
	 * load the dataset into memory
	 */
	public static final String OPTION_IN_MEMORY_DATASET = "qep.dataset.load";
	/**
	 * do not load dataset except the SPO dataset
	 */
	public static final String OPTION_NO_CO_INDEX = "qep.dataset.no_co_index";
	public static final String FILE_DATASET_PREFIX = "index_";
	public static final String FILE_DATASET_SUFFIX = ".hdt";

	private final Map<String, QEPDataset> dataset = new HashMap<>();
	private final Map<QEPMap.Uid, QEPMap> map = new HashMap<>();

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


	private HDT openDataset(Path path) throws IOException {
		if (memoryDataset) {
			if (noCoIndex) {
				return HDTManager.loadHDT(path, ProgressListener.ignore(), options);
			}
			return HDTManager.loadIndexedHDT(path, ProgressListener.ignore(), options);
		} else {
			if (noCoIndex) {
				return HDTManager.mapHDT(path, ProgressListener.ignore(), options);
			}
			return HDTManager.mapIndexedHDT(path, options, ProgressListener.ignore());
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
						QEPDataset ds = new QEPDataset(indexId, path, openDataset(path));
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
	 * bind 2 datasets in the core
	 *
	 * @param dataset1 dataset 1
	 * @param dataset2 dataset 2
	 * @throws IOException bind exception
	 */
	public void bindDataset(QEPDataset dataset1, QEPDataset dataset2) throws IOException {
		if (dataset1.uid() == dataset2.uid()) {
			return;
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
	 * @return the dataset path
	 */
	public Path getDatasetPath() {
		return location.resolve("store");
	}

	/**
	 * @return the maps path
	 */
	public Path getMapsPath() {
		return location.resolve("maps");
	}

	/**
	 * @return the options file path
	 */
	public Path getOptionsPath() {
		return location.resolve("config.opt");
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
}

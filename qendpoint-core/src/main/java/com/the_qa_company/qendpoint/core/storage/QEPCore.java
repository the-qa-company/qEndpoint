package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap64Big;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.TempHDT;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTImpl;
import com.the_qa_company.qendpoint.core.header.HeaderUtil;
import com.the_qa_company.qendpoint.core.iterator.utils.MapFilterIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.storage.converter.NodeConverter;
import com.the_qa_company.qendpoint.core.storage.iterator.CatQueryCloseable;
import com.the_qa_company.qendpoint.core.storage.iterator.QueryCloseableIterator;
import com.the_qa_company.qendpoint.core.storage.merge.QEPCoreMergeThread;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.ContainerException;
import com.the_qa_company.qendpoint.core.util.debug.DebugInjectionPointManager;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.nsd.NamespaceData;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.apache.commons.io.file.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
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
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY;
import static com.the_qa_company.qendpoint.core.options.HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH;

/**
 * qEndpoint's core
 *
 * @author Antoine Willerval
 */
public class QEPCore implements AutoCloseable {

	static final DebugInjectionPointManager.DebugInjectionPoint<QEPCore> preBindInsert = DebugInjectionPointManager.getInstance().registerInjectionPoint("preBindInsert");
	static final DebugInjectionPointManager.DebugInjectionPoint<QEPCore> postBindInsert = DebugInjectionPointManager.getInstance().registerInjectionPoint("postBindInsert");
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
	 * threads count for the executor
	 */
	public static final String OPTION_EXECUTOR_THREADS = "qep.executor.threads";
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
	private final Object datasetLock = new Object() {};
	private final ReentrantLock insertLock = new ReentrantLock();
	private final Object bindLock = new Object() {};
	private final Object idBuilderLock = new Object() {};
	private final ConcurrentMap<Integer, QEPDataset> datasetByUid = new ConcurrentHashMap<>();
	private final ConcurrentMap<Uid, QEPMap> map = new ConcurrentHashMap<>();

	// config
	private final HDTOptions options;
	private final boolean memoryDataset;
	private final boolean noCoIndex;
	private final Path location;
	private ProgressListener listener = ProgressListener.ignore();
	private long maxId;
	private final QEPCoreMergeThread mergeThread;
	private final NamespaceData namespaceData;
	private final ExecutorService executorService;

	QEPCore() {
		executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		options = HDTOptions.of();
		memoryDataset = false;
		noCoIndex = false;
		location = Path.of("tests");
		mergeThread = new QEPCoreMergeThread(this, options);
		namespaceData = new NamespaceData(getNamespaceDataLocation());
	}

	/**
	 * QEP core
	 *
	 * @param location location of the core
	 * @throws QEPCoreException can't init the core
	 */
	public QEPCore(Path location) throws QEPCoreException {
		this(location, HDTOptions.empty(), true);
	}

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

		int threads = opt.getInt32(OPTION_EXECUTOR_THREADS, () -> {
			int nthreads = Runtime.getRuntime().availableProcessors();
			return nthreads <= 0 ? 1 : nthreads; // at least one
		});

		if (threads <= 0) {
			throw new IllegalArgumentException(
					"Can't have a negative or null thread count with " + OPTION_EXECUTOR_THREADS + ": " + threads);
		}

		executorService = Executors.newFixedThreadPool(threads);

		// copy the options in a push-bottom way to set default options
		this.options = opt.pushBottom();

		Path workDir = location.resolve("work");

		try {
			PathUtils.deleteDirectory(workDir);
		} catch (NoSuchFileException ignore) {
			// ignored
		} catch (IOException e) {
			logger.error("Tried to delete previous work directory, but an exception occurred", e);
		}

		this.options.setOptions(
				// set our dictionary type
				DICTIONARY_TYPE_KEY, DICTIONARY_TYPE_VALUE_MULTI_OBJECTS,
				// use msd temp for insert
				TEMP_DICTIONARY_IMPL_KEY, TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH,

				// set the disk indexing
				BITMAPTRIPLES_INDEX_METHOD_KEY, BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK,
				// with the disk sequence
				BITMAPTRIPLES_SEQUENCE_DISK, true,
				// sub index
				BITMAPTRIPLES_SEQUENCE_DISK_SUBINDEX, true,
				// indexing work location
				BITMAPTRIPLES_SEQUENCE_DISK_LOCATION, workDir,

				// generation using cat
				LOADER_TYPE_KEY, LOADER_TYPE_VALUE_CAT,
				// gen location
				LOADER_DISK_LOCATION_KEY, location.resolve("gen"),
				// gen temp dataset
				LOADER_DISK_FUTURE_HDT_LOCATION_KEY, location.resolve("gen-wip.hdt"),

				LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY, location.resolve("cat-wip.hdt"), LOADER_CATTREE_LOADERTYPE_KEY,
				LOADER_TYPE_VALUE_DISK, LOADER_CATTREE_LOCATION_KEY, location.resolve("cattree"),
				LOADER_CATTREE_MEMORY_FAULT_FACTOR, 1, LOADER_CATTREE_KCAT, 20,

				HDTCAT_DELETE_LOCATION, true, HDTCAT_LOCATION, location.resolve("hdtcat"), HDTCAT_FUTURE_LOCATION,
				location.resolve("catgen.hdt"));

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

		mergeThread = new QEPCoreMergeThread(this, options);

		namespaceData = new NamespaceData(getNamespaceDataLocation());

		try {
			// load the dataset, sync the maps and load the namespaces
			reloadDataset();
			syncDatasetMaps();
			namespaceData.load();
		} catch (Throwable t) {
			try {
				close();
			} catch (Throwable t2) {
				t.addSuppressed(t2);
			}
			throw t;
		}

		mergeThread.start();
	}

	/**
	 * @return the number of triples
	 */
	public long triplesCount() {
		return createDatasetSnapshot().stream().mapToLong(d -> d.dataset().getTriples().getNumberOfElements()).sum();
	}

	@SuppressWarnings("resource")
	private QEPDataset openDataset(String id, Path path) throws IOException {
		HDT dataset = null;
		ModifiableBitmap bitmap = null;
		ModifiableBitmap[] deltaBitmaps = new ModifiableBitmap[TripleComponentRole.valuesNoGraph().length];
		try {
			// avoid loading collisions
			Path workDir = options.getPath(BITMAPTRIPLES_SEQUENCE_DISK_LOCATION, () -> location.resolve("work"))
					.resolve(id);
			HDTOptions options = this.options.pushTop();
			options.set(BITMAPTRIPLES_SEQUENCE_DISK_LOCATION, workDir);
			if (memoryDataset) {
				if (noCoIndex) {
					dataset = HDTManager.loadHDT(path, ProgressListener.ignore(), this.options);
				} else {
					dataset = HDTManager.loadIndexedHDT(path, ProgressListener.ignore(), this.options);
				}
			} else {
				if (noCoIndex) {
					dataset = HDTManager.mapHDT(path, ProgressListener.ignore(), this.options);
				} else {
					dataset = HDTManager.mapIndexedHDT(path, this.options, ProgressListener.ignore());
				}
			}

			Path deleteBitmapPath = path.resolveSibling(path.getFileName() + ".delete.bm");

			if (Files.exists(deleteBitmapPath)) {
				bitmap = Bitmap64Big.map(deleteBitmapPath, dataset.getTriples().getNumberOfElements());
			} else {
				bitmap = Bitmap64Big.disk(deleteBitmapPath, dataset.getTriples().getNumberOfElements());
			}

			for (TripleComponentRole role : TripleComponentRole.valuesNoGraph()) {
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
		synchronized (datasetLock) {
			try {
				// clear the pool
				Closer.closeAll(dataset);
				dataset.clear();
				datasetByUid.clear();
				Files.createDirectories(getDatasetPath());
				try (Stream<Path> files = Files.list(getDatasetPath())) {
					files.forEach(path -> {
						String filename = path.getFileName().toString();

						if (!filename.startsWith(FILE_DATASET_PREFIX) || !filename.endsWith(FILE_DATASET_SUFFIX)) {
							// we ignore this file because it's most likely not
							// what
							// we're searching for
							return;
						}

						String indexId = filename.substring(FILE_DATASET_PREFIX.length(),
								filename.length() - FILE_DATASET_SUFFIX.length()).toLowerCase();

						Matcher matcher = ID_REGEX.matcher(indexId);
						if (!matcher.matches()) {
							logger.warn(
									"file {} seems to be a dataset, but isn't matching the id format, it will be ignored.",
									path);
							return;
						}
						// load the dataset
						try {
							if (indexId.length() < 15) {
								try {
									maxId = Math.max(Long.parseLong(indexId), maxId);
								} catch (NumberFormatException ignore) {
								}
							}
							QEPDataset ds = openDataset(indexId, path);
							QEPDataset other = dataset.put(ds.id(), ds);

							// Windows compatibility, it would also be a bad
							// practice to use datasets
							// with case-sensitive names
							if (other != null) {
								ContainerException e = new ContainerException(
										new QEPCoreException("Dataset collision with name " + other.id() + " at path "
												+ other.path() + " vs " + ds.path()));
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
	}

	/**
	 * sync all the maps of the dataset
	 *
	 * @throws QEPCoreException sync exception
	 */
	public void syncDatasetMaps() throws QEPCoreException {
		Path mapsDir = getMapsPath();
		synchronized (bindLock) {
			List<QEPDataset> snapshot = createDatasetSnapshot();
			try {
				// close the previous maps
				Closer.closeAll(map.values());
				map.clear();
				Files.createDirectories(mapsDir);

				List<Future<?>> futures = new ArrayList<>();

				for (QEPDataset d1 : snapshot) {
					for (QEPDataset d2 : snapshot) {
						Future<?> future = bindDataset(d1, d2);
						if (future != null) {
							futures.add(future);
						}
					}
				}

				QEPCoreException exp = null;

				for (Future<?> future : futures) {
					try {
						future.get();
					} catch (InterruptedException | ExecutionException e) {
						if (exp == null) {
							exp = new QEPCoreException(e);
						} else {
							exp.addSuppressed(new QEPCoreException(e));
						}
					}
				}
				if (exp != null) {
					throw exp;
				}
			} catch (IOException e) {
				throw new QEPCoreException("Can't sync map data!", e);
			}
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
	public NodeConverter getConverter(int originUID, int destinationUID, TripleComponentRole role)
			throws QEPCoreException {
		Uid uid = Uid.of(originUID, destinationUID);

		QEPMap qepMap = map.get(uid);
		if (qepMap == null) {
			throw new QEPCoreException("Can't find map with UID " + uid);
		}

		return qepMap.getConverter(originUID, destinationUID, role);
	}

	/**
	 * @return namespace data linked with this core
	 */
	public NamespaceData getNamespaceData() {
		return namespaceData;
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
	 * @return unmodifiable collection of the mappers in the core
	 */
	public Collection<QEPMap> getMappers() {
		return Collections.unmodifiableCollection(map.values());
	}

	/**
	 * @return all the dataset UIDs
	 */
	public Set<Integer> getDatasetUids() {
		return Collections.unmodifiableSet(datasetByUid.keySet());
	}

	/**
	 * @return the datasets
	 */
	public List<QEPDataset> getDatasets() {
		return createDatasetSnapshot();
	}

	/**
	 * bind 2 datasets in the core
	 *
	 * @param dataset1 dataset 1
	 * @param dataset2 dataset 2
	 * @throws IOException bind exception
	 */
	private Future<?> bindDataset(QEPDataset dataset1, QEPDataset dataset2) throws IOException {
		if (dataset1.uid() == dataset2.uid()) {
			return null; // same object
		}

		if (dataset1.id().equals(dataset2.id())) {
			return null; // same id
		}

		// create a map for our 2 datasets
		QEPMap qepMap = new QEPMap(getMapsPath(), this, dataset1, dataset2);

		// try to add this map to our maps
		QEPMap old = map.putIfAbsent(qepMap.getUid(), qepMap);

		if (old == null) {
			// it was added, meaning we need to sync it
			return executorService.submit(() -> {
				try {
					qepMap.sync();
				} catch (IOException e) {
					throw new ContainerException(e);
				}
			});
		}
		return null;
	}

	private List<QEPDataset> createDatasetSnapshot() {
		synchronized (datasetLock) {
			return dataset.values().stream().toList();
		}
	}

	/**
	 * search all the triples into the core
	 *
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public QueryCloseableIterator search() throws QEPCoreException {
		QEPCoreContext ctx = createSearchContext();
		return search(ctx).attach(ctx);
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
	public QueryCloseableIterator search(CharSequence subject, CharSequence predicate, CharSequence object)
			throws QEPCoreException {
		QEPCoreContext ctx = createSearchContext();
		return search(ctx, subject, predicate, object).attach(ctx);
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
	public QueryCloseableIterator search(QEPComponent subject, QEPComponent predicate, QEPComponent object)
			throws QEPCoreException {
		QEPCoreContext ctx = createSearchContext();
		return search(ctx, subject, predicate, object).attach(ctx);
	}

	/**
	 * search a triple into the core
	 *
	 * @param triple triple to search
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public QueryCloseableIterator search(TripleString triple) throws QEPCoreException {
		QEPCoreContext ctx = createSearchContext();
		return search(ctx, triple).attach(ctx);
	}

	/**
	 * search a triple into the core
	 *
	 * @param triple triple to search
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public QueryCloseableIterator search(QEPComponentTriple triple) throws QEPCoreException {
		QEPCoreContext ctx = createSearchContext();
		return search(ctx, triple).attach(ctx);
	}

	/**
	 * search all the triples into the core
	 *
	 * @param context search context
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public QueryCloseableIterator search(QEPCoreContext context) throws QEPCoreException {
		return search(context, "", "", "");
	}

	/**
	 * search a triple into the core
	 *
	 * @param context   search context
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public QueryCloseableIterator search(QEPCoreContext context, CharSequence subject, CharSequence predicate,
			CharSequence object) throws QEPCoreException {
		return search(context, createComponentByString(subject), createComponentByString(predicate),
				createComponentByString(object));
	}

	/**
	 * search a triple into the core
	 *
	 * @param context   search context
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public QueryCloseableIterator search(QEPCoreContext context, QEPComponent subject, QEPComponent predicate,
			QEPComponent object) throws QEPCoreException {
		return search(context, QEPComponentTriple.of(subject, predicate, object));
	}

	/**
	 * search a triple into the core
	 *
	 * @param context search context
	 * @param triple  triple to search
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public QueryCloseableIterator search(QEPCoreContext context, TripleString triple) throws QEPCoreException {
		return search(context, triple.getSubject().isEmpty() ? null : triple.getSubject(),
				triple.getPredicate().isEmpty() ? null : triple.getPredicate(),
				triple.getObject().isEmpty() ? null : triple.getObject());
	}

	/**
	 * search a triple into the core
	 *
	 * @param context search context
	 * @param triple  triple to search
	 * @return iterator of components
	 * @throws QEPCoreException search exception
	 */
	public QueryCloseableIterator search(QEPCoreContext context, QEPComponentTriple triple) throws QEPCoreException {
		List<QueryCloseableIterator> iterators = new ArrayList<>();
		QEPComponentTriple clone = triple.freeze();
		for (QEPDatasetContext dsctx : context.getContexts()) {
			iterators.add(dsctx.dataset().search(dsctx, clone));
		}

		// cat all the iterators
		return CatQueryCloseable.of(iterators);
	}

	/**
	 * search any triple into the core
	 *
	 * @return find something
	 * @throws QEPCoreException search exception
	 */
	public boolean containsAny() throws QEPCoreException {
		try (QueryCloseableIterator it = search()) {
			return it.hasNext();
		}
	}

	/**
	 * search a triple into the core
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return find something
	 * @throws QEPCoreException search exception
	 */
	public boolean containsAny(CharSequence subject, CharSequence predicate, CharSequence object)
			throws QEPCoreException {
		try (QueryCloseableIterator it = search(subject, predicate, object)) {
			return it.hasNext();
		}
	}

	/**
	 * search a triple into the core
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return find something
	 * @throws QEPCoreException search exception
	 */
	public boolean containsAny(QEPComponent subject, QEPComponent predicate, QEPComponent object)
			throws QEPCoreException {
		try (QueryCloseableIterator it = search(subject, predicate, object)) {
			return it.hasNext();
		}
	}

	/**
	 * search a triple into the core
	 *
	 * @param triple triple to search
	 * @return find something
	 * @throws QEPCoreException search exception
	 */
	public boolean containsAny(TripleString triple) throws QEPCoreException {
		try (QueryCloseableIterator it = search(triple)) {
			return it.hasNext();
		}
	}

	/**
	 * search a triple into the core
	 *
	 * @param triple triple to search
	 * @return find something
	 * @throws QEPCoreException search exception
	 */
	public boolean containsAny(QEPComponentTriple triple) throws QEPCoreException {
		try (QueryCloseableIterator it = search(triple)) {
			return it.hasNext();
		}
	}

	/**
	 * search any triple into the core
	 *
	 * @param context search context
	 * @return find something
	 * @throws QEPCoreException search exception
	 */
	public boolean containsAny(QEPCoreContext context) throws QEPCoreException {
		try (QueryCloseableIterator it = search(context)) {
			return it.hasNext();
		}
	}

	/**
	 * search a triple into the core
	 *
	 * @param context   search context
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return find something
	 * @throws QEPCoreException search exception
	 */
	public boolean containsAny(QEPCoreContext context, CharSequence subject, CharSequence predicate,
			CharSequence object) throws QEPCoreException {
		try (QueryCloseableIterator it = search(context, subject, predicate, object)) {
			return it.hasNext();
		}
	}

	/**
	 * search a triple into the core
	 *
	 * @param context   search context
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return find something
	 * @throws QEPCoreException search exception
	 */
	public boolean containsAny(QEPCoreContext context, QEPComponent subject, QEPComponent predicate,
			QEPComponent object) throws QEPCoreException {
		try (QueryCloseableIterator it = search(context, subject, predicate, object)) {
			return it.hasNext();
		}
	}

	/**
	 * search a triple into the core
	 *
	 * @param context search context
	 * @param triple  triple to search
	 * @return find something
	 * @throws QEPCoreException search exception
	 */
	public boolean containsAny(QEPCoreContext context, TripleString triple) throws QEPCoreException {
		try (QueryCloseableIterator it = search(context, triple)) {
			return it.hasNext();
		}
	}

	/**
	 * search a triple into the core
	 *
	 * @param context search context
	 * @param triple  triple to search
	 * @return find something
	 * @throws QEPCoreException search exception
	 */
	public boolean containsAny(QEPCoreContext context, QEPComponentTriple triple) throws QEPCoreException {
		try (QueryCloseableIterator it = search(context, triple)) {
			return it.hasNext();
		}
	}

	/**
	 * remove a triple pattern from the core
	 *
	 * @param subject   subject, empty for wildcard
	 * @param predicate predicate, empty for wildcard
	 * @param object    object, empty for wildcard
	 * @return count of deleted triples
	 * @throws QEPCoreException remove exception
	 */
	public long removeTriple(CharSequence subject, CharSequence predicate, CharSequence object)
			throws QEPCoreException {
		try (QEPCoreContext ctx = createSearchContext()) {
			return removeTriple(ctx, subject, predicate, object);
		}
	}

	/**
	 * remove a triple pattern from the core
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return count of deleted triples
	 * @throws QEPCoreException remove exception
	 */
	public long removeTriple(QEPComponent subject, QEPComponent predicate, QEPComponent object)
			throws QEPCoreException {
		try (QEPCoreContext ctx = createSearchContext()) {
			return removeTriple(ctx, subject, predicate, object);
		}
	}

	/**
	 * remove a triple pattern from the core
	 *
	 * @param triple triple pattern to remove
	 * @return count of deleted triples
	 * @throws QEPCoreException remove exception
	 */
	public long removeTriple(TripleString triple) throws QEPCoreException {
		try (QEPCoreContext ctx = createSearchContext()) {
			return removeTriple(ctx, triple);
		}
	}

	/**
	 * remove a triple pattern from the core
	 *
	 * @param triple triple pattern to remove
	 * @return count of deleted triples
	 * @throws QEPCoreException remove exception
	 */
	public long removeTriple(QEPComponentTriple triple) throws QEPCoreException {
		try (QEPCoreContext ctx = createSearchContext()) {
			return removeTriple(ctx, triple);
		}
	}

	/**
	 * remove a triple pattern from the core
	 *
	 * @param context   context
	 * @param subject   subject, empty for wildcard
	 * @param predicate predicate, empty for wildcard
	 * @param object    object, empty for wildcard
	 * @return count of deleted triples
	 * @throws QEPCoreException remove exception
	 */
	public long removeTriple(QEPCoreContext context, CharSequence subject, CharSequence predicate, CharSequence object)
			throws QEPCoreException {
		return removeTriple(context, createComponentByString(subject), createComponentByString(predicate),
				createComponentByString(object));
	}

	/**
	 * remove a triple pattern from the core
	 *
	 * @param context   context
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return count of deleted triples
	 * @throws QEPCoreException remove exception
	 */
	public long removeTriple(QEPCoreContext context, QEPComponent subject, QEPComponent predicate, QEPComponent object)
			throws QEPCoreException {
		return removeTriple(context, QEPComponentTriple.of(subject, predicate, object));
	}

	/**
	 * remove a triple pattern from the core
	 *
	 * @param context context
	 * @param triple  triple pattern to remove
	 * @return count of deleted triples
	 * @throws QEPCoreException remove exception
	 */
	public long removeTriple(QEPCoreContext context, TripleString triple) throws QEPCoreException {
		return removeTriple(context, triple.getSubject().isEmpty() ? null : triple.getSubject(),
				triple.getPredicate().isEmpty() ? null : triple.getPredicate(),
				triple.getObject().isEmpty() ? null : triple.getObject());
	}

	/**
	 * remove a triple pattern from the core
	 *
	 * @param context context
	 * @param triple  triple pattern to remove
	 * @return count of deleted triples
	 * @throws QEPCoreException remove exception
	 */
	public long removeTriple(QEPCoreContext context, QEPComponentTriple triple) throws QEPCoreException {
		QEPComponentTriple clone = triple.freeze();
		long[] deleted = new long[] { 0 };

		dataset.values().forEach(ds -> {
			QEPDatasetContext dctx = context.getContextForDataset(ds.uid());
			Iterator<QEPComponentTriple> it = ds.search(dctx, clone);
			while (it.hasNext()) {
				it.next();
				it.remove();
				deleted[0]++;
			}
		});

		return deleted[0];
	}

	/**
	 * estimate the cardinality of a triple
	 *
	 * @param s subject
	 * @param p predicate
	 * @param o object
	 * @return estimation
	 */
	public long cardinality(QEPComponent s, QEPComponent p, QEPComponent o) {
		try (QueryCloseableIterator se = search(s, p, o)) {
			return se.estimateCardinality();
		}
	}

	/**
	 * @return a search context, all the search operations created using this
	 *         context will be done using a non-mutable version of the core.
	 */
	public QEPCoreContext createSearchContext() {
		return new QEPCoreContext(this, createDatasetSnapshot());
	}

	/**
	 * create a return a new dataset id
	 *
	 * @return unique dataset id
	 */
	public String createNewDatasetId() {
		synchronized (idBuilderLock) {
			for (long id = maxId + 1; id < Long.MAX_VALUE; id++) {
				String sid = Long.toString(id);
				if (getDatasetById(sid) != null) {
					continue; // a dataset with this ID already exist, overflow?
				}
				maxId = id;
				return sid;
			}
			// force the name to avoid using an overflow
			for (long id = 0; id < maxId; id++) {
				String sid = Long.toString(id);
				if (getDatasetById(sid) != null) {
					continue;
				}
				return sid;
			}
			throw new AssertionError("too many nodes");
		}
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

	/**
	 * @return an importer
	 */
	public QEPImporter createImporter() {
		return new QEPImporter(this);
	}

	/**
	 * load triples into a new dataset
	 *
	 * @param triples           triples to load
	 * @param baseURI           base URI of the new dataset
	 * @param checkAlreadyExist check if the elements were already in the core
	 * @throws IOException     exception while loading triples
	 * @throws ParserException parsing exception while loading the triples
	 */
	public void insertTriples(Iterator<TripleString> triples, String baseURI, boolean checkAlreadyExist)
			throws IOException, ParserException {
		insertTriples(triples, baseURI, checkAlreadyExist, null);
	}

	/**
	 * load triples into a new dataset, is checkAlreadyExist is set to true,
	 * this task will lock the thread if another insertion is running, if a new
	 * loadData with checkAlreadyExist is started while a loadData without
	 * checkAlreadyExist is running, no check will be done on the loaded data
	 * from the non checkAlreadyExist run.
	 *
	 * @param triples           triples to load
	 * @param baseURI           base URI of the new dataset
	 * @param checkAlreadyExist check if the elements were already in the core
	 * @param listener          listener when loading the dataset
	 * @throws IOException     exception while loading triples
	 * @throws ParserException parsing exception while loading the triples
	 */
	public void insertTriples(Iterator<TripleString> triples, String baseURI, boolean checkAlreadyExist,
			ProgressListener listener) throws IOException, ParserException {
		QEPCoreContext ctx;
		if (checkAlreadyExist) {
			ctx = createSearchContext();
			insertLock.lock();
			triples = MapFilterIterator.of(triples, triple -> {
				try (QueryCloseableIterator it = search(ctx, triple)) {
					if (it.hasNext()) {
						return null;
					}
				}
				return triple;
			});
		} else {
			ctx = null;
		}
		QEPDataset other;
		try {
			if (!triples.hasNext()) {
				return; // it's stupid, but maybe all the new elements are
				// already
				// in the core
			}
			String id;
			Path datasetPath;
			while (true) {
				id = createNewDatasetId();
				datasetPath = getDatasetPath().resolve(FILE_DATASET_PREFIX + id + FILE_DATASET_SUFFIX);

				// we wait until we don't overwrite a dataset, bad config or
				// update
				// of the core while running?
				if (!Files.exists(datasetPath)) {
					break;
				}
				logger.warn(
						"a dataset with the id '{}' was found in te file '{}', but it wasn't registered in the core",
						id, datasetPath);
			}
			// push our custom config to set the future dataset (for disk
			// generation
			// methods)
			HDTOptions genOpt = options.pushTop();
			genOpt.set(HDTCAT_FUTURE_LOCATION, datasetPath.toAbsolutePath());
			genOpt.set(LOADER_DISK_FUTURE_HDT_LOCATION_KEY, datasetPath.toAbsolutePath());
			genOpt.set(LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY, datasetPath.toAbsolutePath());

			ProgressListener combinedListener = this.listener.combine(listener);
			try (HDT dataset = HDTManager.generateHDT(triples, baseURI, genOpt, combinedListener)) {
				dataset.saveToHDT(datasetPath, combinedListener);
			} catch (Throwable t) {
				try {
					// delete the dataset file if it was already used
					Files.deleteIfExists(datasetPath);
				} catch (IOException e) {
					t.addSuppressed(e);
				}
				throw t;
			}

			preBindInsert.runAction(this);

			// we open the dataset because the memory generation is using more
			// memory before being reloaded
			QEPDataset ds = openDataset(id, datasetPath);

			// bind the new dataset with all the previous datasets
			synchronized (bindLock) {
				List<Future<?>> futures = new ArrayList<>();
				for (QEPDataset d2 : createDatasetSnapshot()) {
					Future<?> future = bindDataset(ds, d2);
					if (future != null) {
						futures.add(future);
					}
				}

				QEPCoreException exp = null;

				for (Future<?> future : futures) {
					try {
						future.get();
					} catch (InterruptedException | ExecutionException e) {
						if (exp == null) {
							exp = new QEPCoreException(e);
						} else {
							exp.addSuppressed(new QEPCoreException(e));
						}
					}
				}
				if (exp != null) {
					throw exp;
				}

				synchronized (datasetLock) {
					other = dataset.put(ds.id(), ds);
					datasetByUid.put(ds.uid(), ds);
				}
			}

			postBindInsert.runAction(this);
		} finally {
			if (checkAlreadyExist) {
				insertLock.unlock();
				ctx.close();
			}
		}
		if (other != null) {
			QEPCoreException otherLoadedException = new QEPCoreException(
					"dataset with id '" + other.id() + "' was already loaded");
			try {
				other.close();
			} catch (Exception e) {
				otherLoadedException.addSuppressed(e);
			}
			throw otherLoadedException;
		}
	}

	/**
	 * load triples into a new dataset, is checkAlreadyExist is set to true,
	 * this task will lock the thread if another insertion is running, if a new
	 * loadData with checkAlreadyExist is started while a loadData without
	 * checkAlreadyExist is running, no check will be done on the loaded data
	 * from the non checkAlreadyExist run.
	 *
	 * @param tempHDT           temporary HDT
	 * @param checkAlreadyExist check if the elements were already in the core
	 * @throws IOException exception while loading triples
	 */
	public void insertTriples(TempHDT tempHDT, boolean checkAlreadyExist) throws IOException {
		insertTriples(tempHDT, checkAlreadyExist, null);
	}

	/**
	 * load triples into a new dataset, is checkAlreadyExist is set to true,
	 * this task will lock the thread if another insertion is running, if a new
	 * loadData with checkAlreadyExist is started while a loadData without
	 * checkAlreadyExist is running, no check will be done on the loaded data
	 * from the non checkAlreadyExist run.
	 *
	 * @param modHdt            temporary HDT
	 * @param checkAlreadyExist check if the elements were already in the core
	 * @param listener          listener when loading the dataset
	 * @throws IOException exception while loading triples
	 */
	public void insertTriples(TempHDT modHdt, boolean checkAlreadyExist, ProgressListener listener) throws IOException {

		HDTImpl hdt = new HDTImpl(options);
		QEPDataset other;
		try {
			hdt.loadFromModifiableHDT(modHdt, listener);
			hdt.populateHeaderStructure(modHdt.getBaseURI());

			// Add file size to Header
			try {
				long originalSize = HeaderUtil.getPropertyLong(modHdt.getHeader(), "_:statistics",
						HDTVocabulary.ORIGINAL_SIZE);
				hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, originalSize);
			} catch (NotFoundException e) {
				// ignore
			}
			QEPCoreContext ctx;
			if (checkAlreadyExist) {
				ctx = createSearchContext();
				insertLock.lock();
			} else {
				ctx = null;
			}
			try {
				String id;
				Path datasetPath;
				while (true) {
					id = createNewDatasetId();
					datasetPath = getDatasetPath().resolve(FILE_DATASET_PREFIX + id + FILE_DATASET_SUFFIX);

					// we wait until we don't overwrite a dataset, bad config or
					// update
					// of the core while running?
					if (!Files.exists(datasetPath)) {
						break;
					}
					logger.warn(
							"a dataset with the id '{}' was found in te file '{}', but it wasn't registered in the core",
							id, datasetPath);
				}

				ProgressListener combinedListener = this.listener.combine(listener);
				hdt.saveToHDT(datasetPath, combinedListener);
				hdt.close();
				hdt = null;

				// we open the dataset because the memory generation is using
				// more
				// memory before being reloaded
				QEPDataset ds = openDataset(id, datasetPath);

				// bind the new dataset with all the previous datasets
				synchronized (bindLock) {
					List<Future<?>> futures = new ArrayList<>();
					for (QEPDataset d2 : createDatasetSnapshot()) {
						Future<?> future = bindDataset(ds, d2);
						if (future != null) {
							futures.add(future);
						}
					}

					QEPCoreException exp = null;

					for (Future<?> future : futures) {
						try {
							future.get();
						} catch (InterruptedException | ExecutionException e) {
							if (exp == null) {
								exp = new QEPCoreException(e);
							} else {
								exp.addSuppressed(new QEPCoreException(e));
							}
						}
					}
					if (exp != null) {
						throw exp;
					}
					synchronized (datasetLock) {
						other = dataset.put(ds.id(), ds);
						datasetByUid.put(ds.uid(), ds);
					}
				}
			} finally {
				if (checkAlreadyExist) {
					insertLock.unlock();
					ctx.close();
				}
			}
		} catch (Throwable t) {
			try {
				IOUtil.closeObject(hdt);
			} catch (Throwable t2) {
				t.addSuppressed(t2);
			}
			throw t;
		}
		if (other != null) {
			QEPCoreException otherLoadedException = new QEPCoreException(
					"dataset with id '" + other.id() + "' was already loaded");
			try {
				other.close();
			} catch (Exception e) {
				otherLoadedException.addSuppressed(e);
			}
			throw otherLoadedException;
		}
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
	 * @return the core location
	 */
	public Path getLocation() {
		return location;
	}

	public Path getNamespaceDataLocation() {
		return location.resolve("namespaces.nsd");
	}

	/**
	 * @return an unmodifiable {@link HDTOptions} of the core's options
	 */
	public HDTOptions getOptions() {
		return options.readOnly();
	}

	/**
	 * set the progression listener for this core
	 *
	 * @param listener listener
	 * @see #removeListener()
	 */
	public void setListener(ProgressListener listener) {
		this.listener = Objects.requireNonNull(listener);
	}

	/**
	 * remove the progression listener for this core
	 *
	 * @see #setListener(ProgressListener)
	 */
	public void removeListener() {
		setListener(ProgressListener.ignore());
	}

	/**
	 * @return listener of the core
	 */
	public ProgressListener getListener() {
		return listener;
	}

	@Override
	public void close() throws QEPCoreException {
		synchronized (datasetLock) {
			synchronized (bindLock) {
				mergeThread.interrupt();
				try {
					Closer.closeAll(dataset, map);
				} catch (IOException e) {
					throw new QEPCoreException(e);
				} finally {
					dataset.clear();
					datasetByUid.clear();
					map.clear();
				}
			}
		}
	}

	/**
	 * get a dataset by its id
	 *
	 * @param id the dataset id
	 * @return dataset, or null if undefined in this core
	 */
	public QEPDataset getDatasetById(String id) {
		synchronized (datasetLock) {
			return dataset.get(id);
		}
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
}

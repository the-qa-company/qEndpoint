package com.the_qa_company.qendpoint.compiler;

import com.the_qa_company.qendpoint.compiler.sail.LuceneSailCompiler;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.exception.EndpointStoreException;
import com.the_qa_company.qendpoint.store.experimental.ExperimentalQEndpointSail;
import com.the_qa_company.qendpoint.utils.sail.OptimizingSail;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;
import org.eclipse.rdf4j.sail.helpers.SailConnectionWrapper;
import org.eclipse.rdf4j.sail.helpers.SailWrapper;
import org.eclipse.rdf4j.sail.lmdb.LmdbStore;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SailWrapper containing a compiled sail with
 * {@link com.the_qa_company.qendpoint.compiler.SailCompiler}
 *
 * @author Antoine Willerval
 */
public class CompiledSail extends SailWrapper {
	/**
	 * @return a compiler to create a
	 *         {@link com.the_qa_company.qendpoint.compiler.CompiledSail}
	 */
	public static CompiledSailCompiler compiler() {
		return new CompiledSailCompiler();
	}

	private static final Logger logger = LoggerFactory.getLogger(CompiledSail.class);
	private final CompiledSailOptions options;
	private final NotifyingSail source;
	private final BellowSail bellow;
	private final Set<LuceneSail> luceneSails = new HashSet<>();

	private CompiledSail(CompiledSailCompiler config) throws IOException, SailCompiler.SailCompilerException {
		// use set config?
		if (config.options != null) {
			options = config.options;
		} else {
			options = new CompiledSailOptions();
		}
		// get files or create a basic one
		EndpointFiles files;
		if (config.sourceSail instanceof EndpointStore) {
			files = ((EndpointStore) config.sourceSail).getEndpointFiles();
		} else {
			files = Objects.requireNonNullElseGet(config.endpointFiles,
					() -> new EndpointFiles(Path.of("native-store"), Path.of("hdt-store"), "index_dev.hdt"));
		}

		SailCompiler sailCompiler = new SailCompiler();

		// load the validator
		if (config.validator != null) {
			sailCompiler.setValidator(config.validator);
		}

		// register custom strings
		sailCompiler.registerDirObject(files);
		config.stringObject.forEach(sailCompiler::registerDirObject);
		config.stringConfig.forEach(sailCompiler::registerDirString);

		// register custom configs
		if (config.sailCompilerConfig != null) {
			config.sailCompilerConfig.config(sailCompiler);
		}

		LuceneSailCompiler luceneCompiler = (LuceneSailCompiler) sailCompiler
				.getCompiler(SailCompilerSchema.LUCENE_TYPE);

		// load the options
		if (config.configRDFFile != null) {
			sailCompiler.load(config.configRDFFile);
		} else if (config.configRDFStream != null) {
			try {
				sailCompiler.load(config.configRDFStream, config.configRDFStreamFormat);
			} finally {
				if (config.shouldCloseConfigRDFStream) {
					config.configRDFStream.close();
				}
			}
		} else if (config.configSail != null) {
			sailCompiler.load(config.configSail);
		} else if (config.configModel != null) {
			sailCompiler.load(config.configModel);
		}

		// read the options
		try (SailCompiler.SailCompilerReader reader = sailCompiler.getReader()) {
			options.readOptions(reader);
		}

		// create a source sail if required
		if (config.sourceSail == null) {
			EndpointStore endpoint;
			HDTOptions spec = options.createSpecHDTOptions();
			if (config.hdtSpec != null) {
				spec.setOptions(config.hdtSpec);
			}
			// set the storage
			if (options.getStorageMode().equals(SailCompilerSchema.ENDPOINTSTORE_STORAGE)) {
				if (options.getPassMode().equals(SailCompilerSchema.HDT_TWO_PASS_MODE)) {
					spec.set("loader.type", "two-pass");
				}
				Path hdtIndexLocation = files.getLocationHdtPath().resolve(files.getHDTIndex());
				Files.createDirectories(files.getLocationHdtPath());
				if (!Files.exists(hdtIndexLocation)) {
					try (HDT hdt = HDTManager.generateHDT(new Iterator<>() {
						@Override
						public boolean hasNext() {
							return false;
						}

						@Override
						public TripleString next() {
							return null;
						}
					}, "uri", spec, null)) {
						hdt.saveToHDT(hdtIndexLocation.toAbsolutePath().toString(), null);
					} catch (ParserException e) {
						throw new IOException("Can't parse the RDF file", e);
					}
				}
				endpoint = new EndpointStore(files, spec, false,
						options.getHdtReadMode().equals(SailCompilerSchema.HDT_READ_MODE_LOAD));
				endpoint.setThreshold(options.getEndpointThreshold());
				logger.info(
						"Threshold for triples in Native RDF store: " + options.getEndpointThreshold() + " triples");
				source = endpoint;
			} else if (options.getStorageMode().equals(SailCompilerSchema.NATIVESTORE_STORAGE)) {
				NativeStore store = new NativeStore(new File(files.getLocationNative(), "nativeglobal"));
				if (options.isOptimization()) {
					source = new OptimizingSail(store, store::getFederatedServiceResolver);
				} else {
					source = store;
				}
			} else if (options.getStorageMode().equals(SailCompilerSchema.MEMORYSTORE_STORAGE)) {
				MemoryStore store = new MemoryStore();
				if (options.isOptimization()) {
					source = new OptimizingSail(store, store::getFederatedServiceResolver);
				} else {
					source = store;
				}
			} else if (options.getStorageMode().equals(SailCompilerSchema.LMDB_STORAGE)) {
				LmdbStore store = new LmdbStore(new File(files.getLocationNative(), "lmdb"));
				if (options.isOptimization()) {
					source = new OptimizingSail(store, store::getFederatedServiceResolver);
				} else {
					source = store;
				}
			} else if (options.getStorageMode().equals(SailCompilerSchema.QEPC_STORAGE)) {
				ExperimentalQEndpointSail store = new ExperimentalQEndpointSail(files.getLocationHdtPath());
				if (options.isOptimization()) {
					source = new OptimizingSail(store, store::getFederatedServiceResolver);
				} else {
					source = store;
				}
			} else {
				throw new RuntimeException("Bad storage mode: " + options.getStorageMode());
			}
			logger.info("Using storage mode {}, optimized: {}.", options.getStorageMode(), options.isOptimization());
		} else {
			source = config.sourceSail;
		}

		bellow = new BellowSail(source);
		setBaseSail(sailCompiler.compile(bellow));
		// write the lucene sails
		luceneSails.addAll(luceneCompiler.getSails());
	}

	/**
	 * @return the options of this compiled sail
	 */
	public CompiledSailOptions getOptions() {
		return options;
	}

	/**
	 * @return the source sail of this compiled sail
	 */
	public NotifyingSail getSource() {
		return source;
	}

	/**
	 * reindex all the compiled lucene sails
	 *
	 * @throws SailException see
	 *                       {@link org.eclipse.rdf4j.sail.lucene.LuceneSail#reindex()}
	 */
	public void reindexLuceneSails() throws SailException {
		for (LuceneSail sail : luceneSails) {
			// bypass filtering system to use the source
			NotifyingSail oldSail = sail.getBaseSail();
			try {
				sail.setBaseSail(source);
				sail.reindex();
			} finally {
				sail.setBaseSail(oldSail);
			}

		}
	}

	/**
	 * @return if the sail has a least one lucene sail connected to it
	 */
	public boolean hasLuceneSail() {
		return !luceneSails.isEmpty();
	}

	/**
	 * @return the lucene sails linked with this compiled sail
	 */
	public Set<LuceneSail> getLuceneSails() {
		return luceneSails;
	}

	@Override
	public SailConnection getConnection() throws SailException {
		bellow.beginConnectionBuilding();
		try {
			NotifyingSailConnection sourceConnection = bellow.getConnection();
			SailConnection treeConnection = super.getConnection();
			return new CompiledSailConnection(treeConnection, sourceConnection);
		} finally {
			bellow.endConnectionBuilding();
		}
	}

	/**
	 * ask to dump the store, this method won't be working if the source sail
	 * isn't a qEndpoint sail.
	 *
	 * @param output output
	 * @return if the dump was started
	 * @throws SailException Can't start the dump or the sail isn't a qEndpoint
	 *                       sail
	 */
	public boolean dumpStore(Path output) throws SailException {
		if (!(source instanceof EndpointStore store)) {
			throw new SailException("Can't dump sail of type " + source.getClass());
		}
		return store.dump(new CompiledSailEndpointStoreDump(output, this));
	}

	/**
	 * Compiled Sail connection, allows to get the source connection
	 *
	 * @author Antoine Willerval
	 */
	public static class CompiledSailConnection extends SailConnectionWrapper {
		private final NotifyingSailConnection sourceConnection;

		public CompiledSailConnection(SailConnection wrappedCon, NotifyingSailConnection sourceConnection) {
			super(wrappedCon);
			this.sourceConnection = sourceConnection;
		}

		/**
		 * @return the source connection, won't pass by the compiled nodes
		 */
		public NotifyingSailConnection getSourceConnection() {
			return sourceConnection;
		}
	}

	/**
	 * Sail to ge the connection to the source sail
	 */
	static class BellowSail extends NotifyingSailWrapper {
		private final Lock lock = new ReentrantLock();
		private NotifyingSailConnection connection;

		public BellowSail(NotifyingSail baseSail) {
			super(baseSail);
		}

		public void beginConnectionBuilding() {
			lock.lock();
			try {
				if (connection != null) {
					throw new AssertionError("connection already set");
				}
				connection = super.getConnection();
			} catch (Throwable t) {
				try {
					lock.unlock();
				} catch (Throwable t2) {
					t.addSuppressed(t2);
				}
				throw t;
			}
		}

		@Override
		public NotifyingSailConnection getConnection() throws SailException {
			if (connection == null) {
				throw new EndpointStoreException("Can't read connection without calling begin/end connection building");
			}
			return connection;
		}

		public void endConnectionBuilding() {
			try {
				lock.unlock();
			} finally {
				// closed by the user
				connection = null;
			}
		}
	}

	/**
	 * Compiler class for the
	 * {@link com.the_qa_company.qendpoint.compiler.CompiledSail}
	 *
	 * @author Antoine Willerval
	 */
	public static class CompiledSailCompiler {
		private InputStream configRDFStream;
		private boolean shouldCloseConfigRDFStream;
		private RDFFormat configRDFStreamFormat;
		private Path configRDFFile;
		private Sail configSail;
		private Model configModel;
		private NotifyingSail sourceSail;
		private EndpointFiles endpointFiles;
		private HDTOptions hdtSpec;
		private final Map<String, String> stringConfig = new HashMap<>();
		private final List<Object> stringObject = new ArrayList<>();
		private CompiledSailOptions options;
		private SailCompilerValidator validator;
		private SailCompilerConfig sailCompilerConfig;

		private CompiledSailCompiler() {
		}

		/**
		 * add a custom string config for the parsed string in the config
		 *
		 * @param key   key
		 * @param value value
		 * @return this
		 */
		public CompiledSailCompiler withStringConfig(String key, String value) {
			stringConfig.put(key, value);
			return this;
		}

		/**
		 * add a custom string config object for the parsed string in the config
		 *
		 * @param object the object to add
		 * @return this
		 */
		public CompiledSailCompiler withStringObject(Object object) {
			stringObject.add(object);
			return this;
		}

		/**
		 * set an input stream to load the configs
		 *
		 * @param configRDFStream       the rdf input stream
		 * @param configRDFStreamFormat the rdf format of the stream
		 * @param close                 if the compiler should close the stream
		 *                              after reading
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 * @see #withConfig(java.nio.file.Path)
		 * @see #withConfig(org.eclipse.rdf4j.sail.Sail)
		 * @see #withConfig(org.eclipse.rdf4j.model.Model)
		 */
		public CompiledSailCompiler withConfig(InputStream configRDFStream, RDFFormat configRDFStreamFormat,
				boolean close) {
			this.configRDFStream = Objects.requireNonNull(configRDFStream, "configRDFStream can't be null!");
			this.configRDFStreamFormat = Objects.requireNonNull(configRDFStreamFormat,
					"configRDFStreamFormat can't be null!");
			shouldCloseConfigRDFStream = close;
			return this;
		}

		/**
		 * set an input stream to load the configs
		 *
		 * @param configRDFFile the rdf file
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 * @see #withConfig(java.io.InputStream,
		 *      org.eclipse.rdf4j.rio.RDFFormat, boolean)
		 * @see #withConfig(org.eclipse.rdf4j.sail.Sail)
		 * @see #withConfig(org.eclipse.rdf4j.model.Model)
		 */
		public CompiledSailCompiler withConfig(Path configRDFFile) {
			this.configRDFFile = Objects.requireNonNull(configRDFFile, "configRDFFile can't be null!");
			return this;
		}

		/**
		 * set an input stream to load the configs
		 *
		 * @param configModel the rdf model
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 * @see #withConfig(java.io.InputStream,
		 *      org.eclipse.rdf4j.rio.RDFFormat, boolean)
		 * @see #withConfig(org.eclipse.rdf4j.sail.Sail)
		 * @see #withConfig(java.nio.file.Path)
		 */
		public CompiledSailCompiler withConfig(Model configModel) {
			this.configModel = Objects.requireNonNull(configModel, "configModel can't be null!");
			return this;
		}

		/**
		 * set a sail to read the config
		 * <p>
		 * <b> THIS METHOD ISN'T THE SAME AS
		 * {@link #withSourceSail(org.eclipse.rdf4j.sail.NotifyingSail)}, it
		 * will only use the sail to read the config, not wrapping it!! </b>
		 * </p>
		 *
		 * @param configSail the sail
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 * @see #withConfig(java.io.InputStream,
		 *      org.eclipse.rdf4j.rio.RDFFormat, boolean)
		 * @see #withConfig(java.nio.file.Path)
		 * @see #withConfig(org.eclipse.rdf4j.model.Model)
		 */
		public CompiledSailCompiler withConfig(Sail configSail) {
			this.configSail = Objects.requireNonNull(configSail, "configSail can't be null!");
			return this;
		}

		/**
		 * set the options for the sail, might be overwritten by the config
		 *
		 * @param options the options
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 */
		public CompiledSailCompiler withOptions(CompiledSailOptions options) {
			this.options = Objects.requireNonNull(options, "options can't be null!");
			return this;
		}

		/**
		 * set the validator for the sail compiler, might be overwritten by the
		 * config
		 *
		 * @param validator the validator
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 */
		public CompiledSailCompiler withValidator(SailCompilerValidator validator) {
			this.validator = Objects.requireNonNull(validator, "validator can't be null!");
			return this;
		}

		/**
		 * set the config for the sail compiler
		 *
		 * @param config the config
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 */
		public CompiledSailCompiler withCompilerConfig(SailCompilerConfig config) {
			this.sailCompilerConfig = Objects.requireNonNull(config, "config can't be null!");
			return this;
		}

		/**
		 * set a source sail to wrap with this compiled sail
		 *
		 * @param sourceSail the source sail
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 */
		public CompiledSailCompiler withSourceSail(NotifyingSail sourceSail) {
			this.sourceSail = Objects.requireNonNull(sourceSail, "sourceSail can't be null!");
			return this;
		}

		/**
		 * set the endpoint files for this compiled sail, won't be used if the
		 * source sail is already an
		 * {@link com.the_qa_company.qendpoint.store.EndpointStore}, by default
		 * the values are native-store, hdt-store and index_dev.hdt
		 *
		 * @param endpointFiles files to load
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 */
		public CompiledSailCompiler withEndpointFiles(EndpointFiles endpointFiles) {
			this.endpointFiles = Objects.requireNonNull(endpointFiles, "endpointFiles can't be null!");
			return this;
		}

		/**
		 * set the hdt spec for the endpoint store, won't be used if the source
		 * is defined or if the generated source isn't an endpoint store
		 *
		 * @param hdtSpec the spec
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 */
		public CompiledSailCompiler withHDTSpec(String hdtSpec) {
			HDTOptions spec = HDTOptions.of();
			spec.setOptions(hdtSpec);
			return withHDTSpec(spec);
		}

		/**
		 * set the hdt spec for the endpoint store, won't be used if the source
		 * is defined or if the generated source isn't an endpoint store
		 *
		 * @param hdtSpec the spec
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 */
		public CompiledSailCompiler withHDTSpec(HDTOptions hdtSpec) {
			this.hdtSpec = Objects.requireNonNull(hdtSpec, "hdtSpec can't be null!");
			return this;
		}

		/**
		 * compile this sail
		 *
		 * @return compiled sail
		 * @throws IOException                        error while reading a
		 *                                            file/stream
		 * @throws SailCompiler.SailCompilerException compiler exception
		 */
		public CompiledSail compile() throws IOException, SailCompiler.SailCompilerException {
			return new CompiledSail(this);
		}

		/**
		 * compile this sail
		 *
		 * @return compiled repository sail
		 * @throws IOException                        error while reading a
		 *                                            file/stream
		 * @throws SailCompiler.SailCompilerException compiler exception
		 */
		public SparqlRepository compileToSparqlRepository() throws IOException, SailCompiler.SailCompilerException {
			return new SparqlRepository(compile());
		}
	}
}

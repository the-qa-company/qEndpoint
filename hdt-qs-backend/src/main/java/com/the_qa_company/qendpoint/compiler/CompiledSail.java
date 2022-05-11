package com.the_qa_company.qendpoint.compiler;

import com.the_qa_company.qendpoint.compiler.sail.LuceneSailCompiler;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.utils.sail.OptimizingSail;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.helpers.SailWrapper;
import org.eclipse.rdf4j.sail.lmdb.LmdbStore;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * SailWrapper containing a compiled sail with {@link com.the_qa_company.qendpoint.compiler.SailCompiler}
 *
 * @author Antoine Willerval
 */
public class CompiledSail extends SailWrapper {
	/**
	 * @return a compiler to create a {@link com.the_qa_company.qendpoint.compiler.CompiledSail}
	 */
	public static CompiledSailCompiler compiler() {
		return new CompiledSailCompiler();
	}

	private static final Logger logger = LoggerFactory.getLogger(CompiledSail.class);
	private final CompiledSailOptions options = new CompiledSailOptions();
	private final NotifyingSail source;
	private final Set<LuceneSail> luceneSails = new HashSet<>();

	private CompiledSail(CompiledSailCompiler config) throws IOException, SailCompiler.SailCompilerException {
		// get files or create a basic one
		EndpointFiles files;
		if (config.sourceSail instanceof EndpointStore) {
			files = ((EndpointStore) config.sourceSail).getEndpointFiles();
		} else {
			files = Objects.requireNonNullElseGet(config.endpointFiles, () -> new EndpointFiles(
					Path.of("native-store"),
					Path.of("hdt-store"),
					"index_dev.hdt"
			));
		}
		SailCompiler sailCompiler = new SailCompiler();
		sailCompiler.registerDirObject(files);
		LuceneSailCompiler luceneCompiler = (LuceneSailCompiler) sailCompiler.getCompiler(SailCompilerSchema.LUCENE_TYPE);
		luceneCompiler.reset();

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
		}

		// read the options
		try (SailCompiler.SailCompilerReader reader = sailCompiler.getReader()) {
			options.readOptions(reader);
		}

		// create a source sail if required
		if (config.sourceSail == null) {
			EndpointStore endpoint;
			HDTSpecification spec = Objects.requireNonNullElseGet(config.spec, HDTSpecification::new);
			if (config.hdtSpec != null) {
				spec.setOptions(config.hdtSpec);
			}
			// set the storage
			if (options.storageMode.equals(SailCompilerSchema.ENDPOINTSTORE_STORAGE)) {
				if (options.passMode.equals(SailCompilerSchema.HDT_TWO_PASS_MODE)) {
					spec.set("loader.type", "two-pass");
				}
				endpoint = new EndpointStore(files, spec, false, options.hdtReadMode.equals(SailCompilerSchema.HDT_READ_MODE_LOAD));
				endpoint.setThreshold(options.endpointThreshold);
				logger.info("Threshold for triples in Native RDF store: " + options.endpointThreshold + " triples");
				source = endpoint;
			} else if (options.storageMode.equals(SailCompilerSchema.NATIVESTORE_STORAGE)) {
				NativeStore store = new NativeStore(new File(files.getLocationNative(), "nativeglobal"));
				if (options.optimization) {
					source = new OptimizingSail(store, store::getFederatedServiceResolver);
				} else {
					source = store;
				}
			} else if (options.storageMode.equals(SailCompilerSchema.MEMORYSTORE_STORAGE)) {
				MemoryStore store = new MemoryStore();
				if (options.optimization) {
					source = new OptimizingSail(store, store::getFederatedServiceResolver);
				} else {
					source = store;
				}
			} else if (options.storageMode.equals(SailCompilerSchema.LMDB_STORAGE)) {
				LmdbStore store = new LmdbStore(new File(files.getLocationNative(), "lmdb"));
				if (options.optimization) {
					source = new OptimizingSail(store, store::getFederatedServiceResolver);
				} else {
					source = store;
				}
			} else {
				throw new RuntimeException("Bad storage mode: " + options.storageMode);
			}
			logger.info("Using storage mode {}, optimized: {}.", options.storageMode, options.optimization);
		} else {
			source = config.sourceSail;
		}

		// write the lucene sails
		luceneSails.addAll(luceneCompiler.getSails());
		setBaseSail(sailCompiler.compile(source));
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
	 * @throws Exception see {@link org.eclipse.rdf4j.sail.lucene.LuceneSail#reindex()}
	 */
	public void reindexLuceneSails() throws Exception {
		for (LuceneSail sail : luceneSails) {
			sail.reindex();
		}
	}

	/**
	 * Compiler class for the {@link com.the_qa_company.qendpoint.compiler.CompiledSail}
	 *
	 * @author Antoine Willerval
	 */
	public static class CompiledSailCompiler {
		private InputStream configRDFStream;
		private boolean shouldCloseConfigRDFStream;
		private RDFFormat configRDFStreamFormat;
		private Path configRDFFile;
		private Sail configSail;
		private NotifyingSail sourceSail;
		private EndpointFiles endpointFiles;
		private HDTSpecification spec;
		private String hdtSpec;

		private CompiledSailCompiler() {
		}

		/**
		 * set an input stream to load the configs
		 *
		 * @param configRDFStream       the rdf input stream
		 * @param configRDFStreamFormat the rdf format of the stream
		 * @param close                 if the compiler should close the stream after reading
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 * @see #withConfig(java.nio.file.Path)
		 * @see #withConfig(org.eclipse.rdf4j.sail.Sail)
		 */
		public CompiledSailCompiler withConfig(InputStream configRDFStream, RDFFormat configRDFStreamFormat, boolean close) {
			this.configRDFStream = Objects.requireNonNull(configRDFStream, "configRDFStream can't be null!");
			this.configRDFStreamFormat = Objects.requireNonNull(configRDFStreamFormat, "configRDFStreamFormat can't be null!");
			shouldCloseConfigRDFStream = close;
			return this;
		}

		/**
		 * set an input stream to load the configs
		 *
		 * @param configRDFFile the rdf file
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 * @see #withConfig(java.io.InputStream, org.eclipse.rdf4j.rio.RDFFormat, boolean)
		 * @see #withConfig(org.eclipse.rdf4j.sail.Sail)
		 */
		public CompiledSailCompiler withConfig(Path configRDFFile) {
			this.configRDFFile = Objects.requireNonNull(configRDFFile, "configRDFFile can't be null!");
			return this;
		}

		/**
		 * set a sail to read the config
		 * <p><b>
		 * THIS METHOD ISN'T THE SAME AS {@link #withSourceSail(org.eclipse.rdf4j.sail.NotifyingSail)}, it will only
		 * use the sail to read the config, not wrapping it!!
		 * </b></p>
		 *
		 * @param configSail the sail
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 * @see #withConfig(java.io.InputStream, org.eclipse.rdf4j.rio.RDFFormat, boolean)
		 * @see #withConfig(java.nio.file.Path)
		 */
		public CompiledSailCompiler withConfig(Sail configSail) {
			this.configSail = Objects.requireNonNull(configSail, "configSail can't be null!");
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
		 * set the endpoint files for this compiled sail, won't be used if the source sail is already an
		 * {@link com.the_qa_company.qendpoint.store.EndpointStore}, by default the values are native-store, hdt-store
		 * and index_dev.hdt
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
		 * set the hdt spec for the endpoint store, won't be used if the source is defined or if the generated source
		 * isn't an endpoint store
		 * @param spec the spec
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 */
		public CompiledSailCompiler withHDTSpec(HDTSpecification spec) {
			this.spec = Objects.requireNonNull(spec, "spec can't be null!");
			return this;
		}

		/**
		 * set the hdt spec for the endpoint store, won't be used if the source is defined or if the generated source
		 * isn't an endpoint store
		 * @param hdtSpec the spec
		 * @return this
		 * @throws java.lang.NullPointerException a parameter is null
		 */
		public CompiledSailCompiler withHDTSpec(String hdtSpec) {
			this.hdtSpec = Objects.requireNonNull(hdtSpec, "hdtSpec can't be null!");
			return this;
		}

		/**
		 * compiled this sail
		 * @return compiled sail
		 * @throws IOException error while reading a file/stream
		 * @throws SailCompiler.SailCompilerException compiler exception
		 */
		public CompiledSail compile() throws IOException, SailCompiler.SailCompilerException {
			return new CompiledSail(this);
		}
	}
}

package com.the_qa_company.qendpoint.controller;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.the_qa_company.qendpoint.client.QEndpointClient;
import com.the_qa_company.qendpoint.client.SplitStream;
import com.the_qa_company.qendpoint.compiler.CompiledSail;
import com.the_qa_company.qendpoint.compiler.CompiledSailOptions;
import com.the_qa_company.qendpoint.compiler.SailCompilerSchema;
import com.the_qa_company.qendpoint.compiler.SparqlRepository;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.EndpointStoreUtils;
import com.the_qa_company.qendpoint.utils.FileUtils;
import com.the_qa_company.qendpoint.utils.RDFStreamUtils;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.NotifyingSail;
import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebInputException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Component
public class Sparql {
	private static boolean redirectDone;

	public static class NamespaceData {
		private String prefix;
		private String name;

		public NamespaceData(String prefix, String name) {
			this.prefix = prefix;
			this.name = name;
		}

		public NamespaceData() {
		}

		public NamespaceData(Namespace ns) {
			this(ns.getPrefix(), ns.getName());
		}

		public String getPrefix() {
			return prefix;
		}

		public String getName() {
			return name;
		}

		public void setPrefix(String prefix) {
			this.prefix = prefix;
		}

		public void setName(String name) {
			this.name = name;
		}

		@JsonIgnore
		public Namespace asRDF4J() {
			return Values.namespace(getPrefix(), getName());
		}
	}

	public static class NamespaceDataResult {
		private Map<String, String> prefixes;

		public NamespaceDataResult(Map<String, String> prefixes) {
			this.prefixes = prefixes;
		}

		public NamespaceDataResult() {
		}

		public Map<String, String> getPrefixes() {
			return prefixes;
		}

		public void setPrefixes(Map<String, String> prefixes) {
			this.prefixes = prefixes;
		}
	}

	public static class MergeRequestResult {
		private final boolean completed;

		public MergeRequestResult(boolean completed) {
			this.completed = completed;
		}

		public boolean isCompleted() {
			return completed;
		}
	}

	public static class LuceneIndexRequestResult {
		private final boolean completed;

		public LuceneIndexRequestResult(boolean completed) {
			this.completed = completed;
		}

		public boolean isCompleted() {
			return completed;
		}
	}

	public static class HasLuceneIndexResult {
		private final boolean hasLuceneIndex;

		public HasLuceneIndexResult(boolean hasLuceneIndex) {
			this.hasLuceneIndex = hasLuceneIndex;
		}

		public boolean isHasLuceneIndex() {
			return hasLuceneIndex;
		}
	}

	public static class IsMergingResult {
		private final boolean merging;

		public IsMergingResult(boolean merging) {
			this.merging = merging;
		}

		public boolean isMerging() {
			return merging;
		}
	}

	public static class IsDumpingResult {
		private final boolean dumping;

		public IsDumpingResult(boolean dumping) {
			this.dumping = dumping;
		}

		public boolean isDumping() {
			return dumping;
		}
	}

	public static class LoadFileResult {
		private boolean loaded;

		public LoadFileResult(boolean loaded) {
			this.loaded = loaded;
		}

		public boolean isLoaded() {
			return loaded;
		}

		public void setLoaded(boolean loaded) {
			this.loaded = loaded;
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(Sparql.class);
	private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyyMMdd-HHmmss");

	private static Path backupIfExists(Path p) throws IOException {
		if (Files.exists(p)) {
			Path old = p.resolveSibling("old");
			Files.createDirectories(old);
			String dateTime = p.getFileName() + "_" + FORMAT.format(Calendar.getInstance().getTime());
			Path next = old.resolve(dateTime);
			logger.info("move old file to {}", next);
			int iteration = 0;
			while (Files.exists(next)) {
				next = old.resolve(dateTime + "_" + ++iteration);
			}
			Files.move(p, next);
		}
		return p;
	}

	@Value("${hdtStoreName}")
	String locationHdtCfg;

	@Value("${hdtIndexName}")
	String hdtIndexName;

	@Value("${nativeStoreName}")
	String locationNativeCfg;

	@Value("${locationEndpoint}")
	String locationEndpointCfg;

	@Value("${threshold}")
	int threshold;

	@Value("${hdtSpecification}")
	String hdtSpec;

	@Value("${repoModel}")
	String repoModel;

	@Value("${maxTimeout}")
	int maxTimeoutCfg;

	@Value("${maxTimeoutUpdate}")
	int maxTimeoutUpdateCfg;

	@Value("${server.port}")
	String portCfg;

	@Value("${qendpoint.client}")
	boolean client;

	EndpointStore endpoint;
	CompiledSail compiledSail;
	SparqlRepository sparqlRepository;
	QEndpointClient qClient;
	final Object storeLock = new Object() {};
	boolean loading = false;
	int queries;
	int port;
	private Path applicationDirectory;
	private Path sparqlPrefixesFile;
	private Path logFile;
	String locationHdt;
	String locationNative;
	boolean init;
	boolean serverInit;

	void waitLoading(int query) {
		synchronized (storeLock) {
			while ((query != 0 && loading) || (query == 0 && queries != 0)) {
				try {
					storeLock.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
					return;
				}
			}
			this.queries += query;
		}
	}

	void startLoading() {
		synchronized (storeLock) {
			loading = true;
		}
	}

	void completeLoading() {
		synchronized (storeLock) {
			this.loading = false;
			storeLock.notifyAll();
		}
	}

	void completeQuery() {
		synchronized (storeLock) {
			queries--;
			assert queries >= 0;
			if (queries == 0) {
				storeLock.notifyAll();
			}
		}
	}

	/**
	 * Init the endpoint
	 *
	 * @throws IOException exception with the initialization
	 */
	public void init() throws IOException {
		initializeEndpointStore(true);
	}

	@PostConstruct
	public void runClient() throws IOException {
		if (client) {
			qClient = new QEndpointClient();
			applicationDirectory = qClient.getApplicationDirectory();
		} else {
			qClient = null;
			applicationDirectory = Path.of(locationEndpointCfg);
		}
		locationHdt = applicationDirectory.resolve(locationHdtCfg).toAbsolutePath() + File.separator;
		locationNative = applicationDirectory.resolve(locationNativeCfg).toAbsolutePath() + File.separator;

		sparqlPrefixesFile = applicationDirectory.resolve("prefixes.sparql");

		// set default value
		port = Integer.parseInt(portCfg);

		redirectOutput(applicationDirectory.resolve("logs").resolve("logs.output"));

		logger.info("using applicationDirectory: {}", applicationDirectory);
		logger.info("locationHdt:                {}", locationHdt);
		logger.info("locationNative:             {}", locationNative);

		init();
	}

	private void redirectOutput(Path file) throws IOException {
		if (redirectDone) {
			return;
		}
		redirectDone = true;
		Files.createDirectories(file.getParent());

		Path out = backupIfExists(file);

		FileOutputStream f1 = null;

		try {
			f1 = new FileOutputStream(out.toFile());

			System.setOut(new PrintStream(SplitStream.of(f1, System.out)));
			System.setErr(new PrintStream(SplitStream.of(f1, System.err)));
		} catch (Exception e) {
			logger.warn("Can't redirect streams", e);
			try {
				if (f1 != null) {
					f1.close();
				}
			} catch (Exception ee) {
				// ignore close error
			}
		}
		logFile = file;
	}

	/**
	 * write the log file into a stream
	 *
	 * @param stream the stream
	 * @throws IOException io error
	 */
	public void writeLogs(OutputStream stream) throws IOException {
		if (logFile == null || Files.notExists(logFile)) {
			return;
		}

		Files.copy(logFile, stream);
	}

	/**
	 * @return the server address
	 */
	public String getServerAddress() {
		return "http://localhost:" + port + "/";
	}

	/**
	 * open the server address in the web navigator
	 */
	public void openClient() {
		if (qClient != null) {
			try {
				qClient.openUri(new URI(getServerAddress()));
			} catch (Exception e) {
				// ignore exception
				e.printStackTrace();
			}
		}
	}

	/**
	 * shutdown the endpoint
	 *
	 * @throws IOException io exception
	 */
	@PreDestroy
	public void shutdown() throws IOException {
		startLoading();
		if (init) {
			logger.info("Clear old store");
			init = false;
			sparqlRepository.shutDown();
			sparqlRepository = null;
			endpoint = null;
		}
	}

	void initializeEndpointStore(boolean finishLoading) throws IOException {
		if (!init) {
			init = true;

			Files.createDirectories(applicationDirectory);

			// keep the config in application.properties
			CompiledSailOptions options = new CompiledSailOptions();
			options.setPort(port);
			options.setEndpointThreshold(threshold);
			options.setHdtSpec(hdtSpec);
			options.setTimeoutQuery(maxTimeoutCfg);
			options.setTimeoutUpdate(maxTimeoutUpdateCfg);

			EndpointFiles files = new EndpointFiles(locationNative, locationHdt, hdtIndexName);

			Path p = applicationDirectory.resolve(repoModel);

			if (Files.notExists(p)) {
				// create config file
				Files.copy(FileUtils.openFile(applicationDirectory, repoModel), p);
			}

			compiledSail = CompiledSail.compiler().withOptions(options)
					.withConfig(Files.newInputStream(p), Rio.getParserFormatForFileName(repoModel).orElseThrow(), true)
					.withEndpointFiles(files).compile();

			NotifyingSail source = compiledSail.getSource();

			if (source instanceof EndpointStore) {
				endpoint = (EndpointStore) source;
			} else {
				assert !compiledSail.getOptions().getStorageMode().equals(SailCompilerSchema.ENDPOINTSTORE_STORAGE);
			}

			sparqlRepository = new SparqlRepository(compiledSail);
			sparqlRepository.init();
			sparqlRepository.readDefaultPrefixes(sparqlPrefixesFile);
			sparqlRepository.saveDefaultPrefixes(sparqlPrefixesFile);

			// set the config
			if (!serverInit) {
				serverInit = true;
				CompiledSailOptions opt = sparqlRepository.getOptions();
				port = opt.getPort();
			}
		}
		if (finishLoading) {
			completeLoading();
		}
	}

	/**
	 * ask for a merge of the endpoint store
	 *
	 * @return see
	 *         {@link com.the_qa_company.qendpoint.store.EndpointStore#mergeStore()}
	 *         return value
	 */
	public MergeRequestResult askForAMerge() {
		if (endpoint == null) {
			throw new ServerWebInputException("No endpoint store, bad config?");
		}
		this.endpoint.mergeStore();
		return new MergeRequestResult(true);
	}

	/**
	 * ask for a merge of the endpoint store
	 *
	 * @return see
	 *         {@link com.the_qa_company.qendpoint.store.EndpointStore#mergeStore()}
	 *         return value
	 */
	public MergeRequestResult askForADump() {
		if (endpoint == null) {
			throw new ServerWebInputException("No endpoint store, bad config?");
		}

		Path outLocation = compiledSail.getOptions().getDumpLocation()
				.resolve(DateTimeFormatter.ofPattern("yyy-MM-dd HHmmss").format(LocalDateTime.now()));
		return new MergeRequestResult(this.sparqlRepository.askDump(outLocation));
	}

	/**
	 * @return if the store is dumping
	 */
	public IsDumpingResult isDumping() {
		if (endpoint == null) {
			throw new ServerWebInputException("No endpoint store, bad config?");
		}
		return new IsDumpingResult(endpoint.isDumping());
	}

	/**
	 * @return if the store is merging
	 */
	public IsMergingResult isMerging() {
		if (endpoint == null) {
			throw new ServerWebInputException("No endpoint store, bad config?");
		}
		return new IsMergingResult(endpoint.isMergeTriggered);
	}

	public LuceneIndexRequestResult reindexLucene() throws Exception {
		initializeEndpointStore(true);
		sparqlRepository.reindexLuceneSails();
		return new LuceneIndexRequestResult(true);
	}

	/**
	 * @return if the sail has a least one lucene sail connected to it
	 */
	public HasLuceneIndexResult hasLuceneSail() {
		return new HasLuceneIndexResult(sparqlRepository.hasLuceneSail());
	}

	public void execute(String sparqlQuery, int timeout, String acceptHeader, Consumer<String> mimeSetter,
			OutputStream out, String queryParam) {
		waitLoading(1);
		try {
			sparqlRepository.execute(sparqlQuery, timeout, acceptHeader, mimeSetter, out, queryParam);
		} finally {
			completeQuery();
		}
	}

	public void executeUpdate(String sparqlQuery, int timeout, OutputStream out) {
		logger.info("timeout: " + timeout);
		// logger.info("Running update query:"+sparqlQuery);
		waitLoading(1);
		try {
			sparqlRepository.executeUpdate(sparqlQuery, timeout, out);
		} finally {
			completeQuery();
		}
	}

	public LoadFileResult loadFile(InputStream input, String filename) throws IOException {
		// wait previous loading
		waitLoading(0);
		try {
			String rdfInput = locationHdt + new File(filename).getName();
			String hdtOutput = EndpointFiles.getHDTIndex(locationHdt, hdtIndexName);
			String baseURI = EndpointStoreUtils.baseURIFromFilename(rdfInput);

			Files.createDirectories(Paths.get(locationHdt));
			Files.deleteIfExists(Paths.get(hdtOutput));
			Files.deleteIfExists(Paths.get(EndpointFiles.getHDTIndexV11(locationHdt, hdtIndexName)));

			if (sparqlRepository.getOptions().getStorageMode().equals(SailCompilerSchema.ENDPOINTSTORE_STORAGE)) {
				shutdown();

				RDFFormat format = Rio.getParserFormatForFileName(filename)
						.orElseThrow(() -> new ServerWebInputException("file format not supported " + filename));

				EndpointStore endpoint = (EndpointStore) compiledSail.getSource();
				EndpointFiles files = endpoint.getEndpointFiles();
				Path hdtStore = files.getLocationHdtPath();
				Path endIndex = hdtStore.resolve(files.getHDTIndex());

				Path backupIndex = endIndex.resolveSibling(endIndex.getFileName() + ".bckp");
				if (Files.exists(endIndex)) {
					// move previous dataset in case of error
					Files.move(endIndex, backupIndex);
				}

				try (CloseSuppressPath hdtStoreWork = CloseSuppressPath.of(hdtStore.resolve("work"))) {
					hdtStoreWork.closeWithDeleteRecurse();
					Files.createDirectories(hdtStoreWork);
					CloseSuppressPath endHDT = hdtStoreWork.resolve("workhdt.hdt");
					Files.deleteIfExists(endHDT);

					HDTOptions opt = compiledSail.getOptions().createHDTOptions(endHDT, hdtStoreWork);

					Iterator<TripleString> stream = RDFStreamUtils.readRDFStreamAsTripleStringIterator(
							IOUtil.asUncompressed(input, CompressionType.guess(filename)), format, true);

					try (HDT hdt = HDTManager.generateHDT(stream, baseURI, opt, this::listener)) {
						if (!Files.exists(endHDT)) {
							// it doesn't exist, probably because someone
							// changed the future location,
							// write manually the file
							hdt.saveToHDT(endHDT.toAbsolutePath().toString(), this::listener);
						}
					} catch (ParserException e) {
						throw new IOException(e);
					}

					Files.move(endHDT, endIndex);
				} finally {
					if (Files.exists(endIndex)) {
						// everything worked
						Files.deleteIfExists(backupIndex);
					} else {
						// error
						if (Files.exists(backupIndex)) {
							Files.move(backupIndex, endIndex);
						}
					}
				}

				initializeEndpointStore(false);
			} else {
				shutdown();
				initializeEndpointStore(false);
				sendUpdates(input, baseURI, filename);
			}
			try {
				sparqlRepository.reindexLuceneSails();
			} catch (Exception e) {
				throw new RuntimeException("Can't reindex the lucene sail(s)!", e);
			}
		} finally {
			completeLoading();
		}
		return new LoadFileResult(true);
	}

	private void listener(float perc, String msg) {
		// ignore
	}

	/**
	 * @return a theoretical maximum amount of memory the JVM will attempt to
	 *         use
	 */
	static long getMaxChunkSize() {
		Runtime runtime = Runtime.getRuntime();
		long presFreeMemory = (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * 0.125
				* 0.85);
		logger.info("Maximal available memory {}", presFreeMemory);
		return presFreeMemory;
	}

	/**
	 * save the prefixes
	 *
	 * @throws IOException write exception
	 */
	public void savePrefixes() throws IOException {
		sparqlRepository.saveDefaultPrefixes(sparqlPrefixesFile);
	}

	public void setPrefixes(Map<String, String> namespaceDataList) throws IOException {
		sparqlRepository.setDefaultPrefixes(namespaceDataList.entrySet().stream()
				.map(e -> Values.namespace(e.getKey(), e.getValue())).collect(Collectors.toList()));
		savePrefixes();
	}

	public Map<String, String> getPrefixes() {
		// use a tree map to have sorted keys
		Map<String, String> prefixes = new TreeMap<>();
		sparqlRepository.getDefaultPrefixes().forEach(p -> prefixes.put(p.getPrefix(), p.getName()));
		return prefixes;
	}

	private void sendUpdates(InputStream inputStream, String baseURI, String filename) throws IOException {
		StopWatch timeWatch = new StopWatch();

		// uncompress the file if required
		InputStream fileStream = RDFStreamUtils.uncompressedStream(inputStream, filename);
		// get a triple iterator for this stream
		Iterator<Statement> it = RDFStreamUtils.readRDFStreamAsIterator(fileStream,
				Rio.getParserFormatForFileName(filename)
						.orElseThrow(() -> new ServerWebInputException("file format not supported " + filename)),
				true);

		long triples = 0;
		long total = 0;
		while (it.hasNext()) {
			try (RepositoryConnection connection = sparqlRepository.getConnection()) {
				connection.begin();
				for (int i = 0; i < sparqlRepository.getOptions().getRdf4jSplitUpdate(); i++) {
					Statement stmt = it.next();
					connection.add(stmt);
					triples++;
					if (!it.hasNext()) {
						break;
					}
				}
				connection.commit();
			}
			if (triples >= 100_000L) {
				total += triples;
				logger.info("loaded {} triples (+{}), {}", total, triples, timeWatch.stopAndShow());
				triples = 0;
			}
		}
		total += triples;
		logger.info("loaded {} triples (+{})", total, triples);

		logger.info("NT file loaded in {}", timeWatch.stopAndShow());
	}

	private void generateHDT(Iterator<TripleString> it, String baseURI, HDTOptions spec, String hdtOutput)
			throws IOException {
		if (sparqlRepository.getOptions().getPassMode().equals(SailCompilerSchema.HDT_TWO_PASS_MODE)) {
			// dump the file to the disk to allow 2 passes
			Path tempNTFile = Paths.get(hdtOutput + "-tmp.nt");
			logger.info("Create TEMP NT file '{}'", tempNTFile);
			try {
				try (PrintWriter stream = new PrintWriter(tempNTFile.toFile())) {
					while (it.hasNext()) {
						TripleString ts = it.next();
						ts.dumpNtriple(stream);
					}
				}
				logger.info("NT file created, generating HDT...");
				try {
					HDT hdtDump = HDTManager.generateHDT(tempNTFile.toFile().getAbsolutePath(), baseURI,
							RDFNotation.NTRIPLES, spec, null);
					hdtDump.saveToHDT(hdtOutput, null);
					hdtDump.close();
				} catch (ParserException e) {
					throw new IOException("Can't generate HDT", e);
				}
			} finally {
				Files.deleteIfExists(tempNTFile);
			}
		} else {
			// directly use the TripleString stream to generate the HDT
			try {
				HDT hdtDump = HDTManager.generateHDT(it, baseURI, spec, null);
				hdtDump.saveToHDT(hdtOutput, null);
				hdtDump.close();
			} catch (ParserException e) {
				throw new IOException("Can't generate HDT", e);
			}
		}
	}

	public int getPort() {
		return port;
	}
}

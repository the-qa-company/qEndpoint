package com.the_qa_company.qendpoint.controller;

import com.the_qa_company.qendpoint.compiler.CompiledSail;
import com.the_qa_company.qendpoint.compiler.CompiledSailOptions;
import com.the_qa_company.qendpoint.compiler.SailCompilerSchema;
import com.the_qa_company.qendpoint.compiler.SparqlRepository;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.utils.FileTripleIterator;
import com.the_qa_company.qendpoint.utils.FileUtils;
import com.the_qa_company.qendpoint.utils.RDFStreamUtils;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebInputException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.function.Consumer;

@Component
public class Sparql {
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

	public static class IsMergingResult {
		private final boolean merging;

		public IsMergingResult(boolean merging) {
			this.merging = merging;
		}

		public boolean isMerging() {
			return merging;
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

	public static int count = 0;
	public static int countEquals = 0;

	private static final String sparqlPrefixes = String.join("\n",
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>", "PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#>",
			"PREFIX dct: <http://purl.org/dc/terms/>", "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>", "PREFIX wikibase: <http://wikiba.se/ontology#>",
			"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>", "PREFIX cc: <http://creativecommons.org/ns#>",
			"PREFIX geo: <http://www.opengis.net/ont/geosparql#>", "PREFIX prov: <http://www.w3.org/ns/prov#>",
			"PREFIX wd: <http://www.wikidata.org/entity/>",
			"PREFIX data: <https://www.wikidata.org/wiki/Special:EntityData/>",
			"PREFIX s: <http://www.wikidata.org/entity/statement/>", "PREFIX ref: <http://www.wikidata.org/reference/>",
			"PREFIX v: <http://www.wikidata.org/value/>", "PREFIX wdt: <http://www.wikidata.org/prop/direct/>",
			"PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/>",
			"PREFIX p: <http://www.wikidata.org/prop/>", "PREFIX ps: <http://www.wikidata.org/prop/statement/>",
			"PREFIX psv: <http://www.wikidata.org/prop/statement/value/>",
			"PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>",
			"PREFIX pq: <http://www.wikidata.org/prop/qualifier/>",
			"PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>",
			"PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>",
			"PREFIX pr: <http://www.wikidata.org/prop/reference/>",
			"PREFIX prv: <http://www.wikidata.org/prop/reference/value/>",
			"PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/>",
			"PREFIX wdno: <http://www.wikidata.org/prop/novalue/> ");
	final HashMap<String, RepositoryConnection> model = new HashMap<>();

	// to test the chunk development of stream
	public long debugMaxChunkSize = -1;
	@Value("${locationHdt}")
	private String locationHdt;

	@Value("${hdtIndexName}")
	private String hdtIndexName;

	@Value("${locationNative}")
	private String locationNative;

	@Value("${threshold}")
	private int threshold;

	@Value("${hdtSpecification}")
	private String hdtSpec;

	@Value("${repoModel}")
	private String repoModel;

	@Value("${maxTimeout}")
	private int maxTimeout;
	@Value("${maxTimeoutUpdate}")
	private int maxTimeoutUpdate;

	EndpointStore endpoint;
	SparqlRepository sparqlRepository;
	final Object storeLock = new Object() {};
	boolean loading = false;
	int queries;

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

	@Autowired
	public void init() throws IOException {
		initializeEndpointStore(locationHdt, true);
	}

	public void clearEndpointStore(String location) throws IOException {
		startLoading();
		if (model.containsKey(location)) {
			logger.info("Clear old store");
			model.remove(location);
			sparqlRepository.shutDown();
			endpoint = null;
			sparqlRepository = null;
		}
		FileUtils.deleteRecursively(Paths.get(locationNative));
	}

	public void initializeEndpointStore(String location, boolean finishLoading) throws IOException {
		if (!model.containsKey(location)) {
			model.put(location, null);
			HDTSpecification spec = new HDTSpecification();
			spec.setOptions(hdtSpec);

			File hdtFile = new File(EndpointFiles.getHDTIndex(location, hdtIndexName));
			if (!hdtFile.exists()) {
				File tempRDF = new File(location + "tmp_index.nt");
				Files.createDirectories(tempRDF.getParentFile().toPath());
				Files.createFile(tempRDF.toPath());
				try {
					HDT hdt = HDTManager.generateHDT(tempRDF.getAbsolutePath(), "uri", RDFNotation.NTRIPLES, spec,
							null);
					hdt.saveToHDT(hdtFile.getPath(), null);
				} catch (ParserException e) {
					throw new IOException("Can't parse the RDF file", e);
				}
				Files.delete(Paths.get(tempRDF.getAbsolutePath()));
			}

			// keep the config in application.properties
			CompiledSailOptions.setDefaultEndpointThreshold(threshold);

			EndpointFiles files = new EndpointFiles(locationNative, locationHdt, hdtIndexName);
			InputStream stream;
			try {
				stream = new FileInputStream(repoModel);
			} catch (IOException e) {
				stream = getClass().getClassLoader().getResourceAsStream(repoModel);
				if (stream == null) {
					throw e;
				}
			}

			CompiledSail compiledSail = CompiledSail.compiler()
					.withConfig(stream, Rio.getParserFormatForFileName(repoModel).orElseThrow(), true)
					.withEndpointFiles(files).withHDTSpec(spec).compile();

			NotifyingSail source = compiledSail.getSource();

			if (source instanceof EndpointStore) {
				endpoint = (EndpointStore) source;
			}

			sparqlRepository = new SparqlRepository(compiledSail);
			sparqlRepository.setSparqlPrefixes(sparqlPrefixes);
			sparqlRepository.init();
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
	 * @return if the store is merging
	 */
	public IsMergingResult isMerging() {
		if (endpoint == null) {
			throw new ServerWebInputException("No enpoint store, bad config?");
		}
		return new IsMergingResult(endpoint.isMergeTriggered);
	}

	public LuceneIndexRequestResult reindexLucene() throws Exception {
		initializeEndpointStore(locationHdt, true);
		sparqlRepository.reindexLuceneSails();
		return new LuceneIndexRequestResult(true);
	}

	public void execute(String sparqlQuery, int timeout, String acceptHeader, Consumer<String> mimeSetter,
			OutputStream out) {
		if (timeout == 0) {
			timeout = maxTimeout;
		} else if (timeout < 0) {
			throw new ServerWebInputException("negative timeout!");
		}
		waitLoading(1);
		try {
			sparqlRepository.execute(sparqlQuery, timeout, acceptHeader, mimeSetter, out);
		} finally {
			completeQuery();
		}
	}

	public void executeUpdate(String sparqlQuery, int timeout, OutputStream out) {
		if (timeout == 0) {
			timeout = maxTimeoutUpdate;
		} else if (timeout < 0) {
			throw new ServerWebInputException("negative timeout!");
		}
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
			String rdfInput = locationHdt + filename;
			String hdtOutput = EndpointFiles.getHDTIndex(locationHdt, hdtIndexName);
			String baseURI = "file://" + rdfInput;

			Files.createDirectories(Paths.get(locationHdt));
			Files.deleteIfExists(Paths.get(hdtOutput));
			Files.deleteIfExists(Paths.get(EndpointFiles.getHDTIndexV11(locationHdt, hdtIndexName)));

			if (sparqlRepository.getOptions().getStorageMode().equals(SailCompilerSchema.ENDPOINTSTORE_STORAGE)) {
				clearEndpointStore(locationHdt);
				HDTSpecification spec = new HDTSpecification();
				spec.setOptions(hdtSpec);
				if (sparqlRepository.getOptions().getPassMode().equals(SailCompilerSchema.HDT_TWO_PASS_MODE)) {
					spec.set("loader.type", "two-pass");
				}
				compressToHdt(input, baseURI, filename, hdtOutput, spec);

				initializeEndpointStore(locationHdt, false);
			} else {
				clearEndpointStore(locationHdt);
				initializeEndpointStore(locationHdt, false);
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

	private void compressToHdt(InputStream inputStream, String baseURI, String filename, String hdtLocation,
			HDTSpecification specs) throws IOException {
		long chunkSize = getMaxChunkSize();

		if (debugMaxChunkSize > 0) {
			assert debugMaxChunkSize < chunkSize : "debugMaxChunkSize can't be higher than chunkSize";
			chunkSize = debugMaxChunkSize;
		}
		File hdtParentFile = new File(hdtLocation).getParentFile();
		String hdtParent = hdtParentFile.getAbsolutePath();
		Files.createDirectories(hdtParentFile.toPath());

		StopWatch timeWatch = new StopWatch();

		File tempFile = new File(filename);
		// the compression will not fit in memory, cat the files in chunks and
		// use hdtCat

		// uncompress the file if required
		InputStream fileStream = RDFStreamUtils.uncompressedStream(inputStream, filename);
		// get a triple iterator for this stream
		Iterator<TripleString> tripleIterator = RDFStreamUtils.readRDFStreamAsTripleStringIterator(fileStream,
				Rio.getParserFormatForFileName(filename)
						.orElseThrow(() -> new ServerWebInputException("file format not supported " + filename)),
				true);
		// split this triple iterator to filed triple iterator
		FileTripleIterator it = new FileTripleIterator(tripleIterator, chunkSize);

		int file = 0;
		String lastFile = null;
		while (it.hasNewFile()) {
			logger.info("Compressing #" + file);
			String hdtOutput = new File(tempFile.getParent(),
					tempFile.getName() + "." + String.format("%03d", file) + ".hdt").getAbsolutePath();

			generateHDT(it, baseURI, specs, hdtOutput);

			System.gc();
			logger.info("Competed into " + hdtOutput);
			if (file > 0) {
				// not the first file, so we have at least 2 files
				logger.info("Cat " + hdtOutput);
				String nextIndex = hdtParent + "/index_cat_tmp_" + file + ".hdt";
				HDT tmp = HDTManager.catHDT(nextIndex, lastFile, hdtOutput, specs, null);

				System.out.println(
						"saving hdt with " + tmp.getTriples().getNumberOfElements() + " triple(s) into " + nextIndex);
				tmp.saveToHDT(nextIndex, null);
				tmp.close();
				System.gc();

				Files.delete(Paths.get(hdtOutput));
				if (file > 1) {
					// at least the 2nd
					Files.delete(Paths.get(hdtParent, "/index_cat_tmp_" + (file - 1) + ".hdt"));
					Files.delete(Paths.get(hdtParent, "/index_cat_tmp_" + (file - 1) + ".hdtdictionary"));
					Files.delete(Paths.get(hdtParent, "/index_cat_tmp_" + (file - 1) + ".hdttriples"));
				} else {
					Files.delete(Paths.get(lastFile));
				}
				lastFile = nextIndex;
			} else {
				lastFile = hdtOutput;
			}
			file++;
		}
		assert lastFile != null : "Last file can't be null";
		Files.move(Paths.get(lastFile), Paths.get(hdtLocation));
		if (file != 1) {
			Files.delete(Paths.get(hdtParent, "/index_cat_tmp_" + (file - 1) + ".hdtdictionary"));
			Files.delete(Paths.get(hdtParent, "/index_cat_tmp_" + (file - 1) + ".hdttriples"));
		}
		logger.info("NT file loaded in {}", timeWatch.stopAndShow());
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

	private void generateHDT(Iterator<TripleString> it, String baseURI, HDTSpecification spec, String hdtOutput)
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
}

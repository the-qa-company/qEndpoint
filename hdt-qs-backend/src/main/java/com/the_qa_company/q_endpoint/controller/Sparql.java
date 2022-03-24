package com.the_qa_company.q_endpoint.controller;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.hybridstore.HybridStoreFiles;
import com.the_qa_company.q_endpoint.utils.FileTripleIterator;
import com.the_qa_company.q_endpoint.utils.FileUtils;
import com.the_qa_company.q_endpoint.utils.FormatUtils;
import com.the_qa_company.q_endpoint.utils.RDFStreamUtils;
import com.the_qa_company.q_endpoint.utils.sail.builder.SailCompiler;
import com.the_qa_company.q_endpoint.utils.sail.builder.SailCompilerSchema;
import com.the_qa_company.q_endpoint.utils.sail.builder.compiler.LuceneSailCompiler;
import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.query.resultio.QueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterRegistry;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebInputException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

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
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>",
			"PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#>",
			"PREFIX dct: <http://purl.org/dc/terms/>",
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>",
			"PREFIX wikibase: <http://wikiba.se/ontology#>",
			"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>",
			"PREFIX cc: <http://creativecommons.org/ns#>",
			"PREFIX geo: <http://www.opengis.net/ont/geosparql#>",
			"PREFIX prov: <http://www.w3.org/ns/prov#>",
			"PREFIX wd: <http://www.wikidata.org/entity/>",
			"PREFIX data: <https://www.wikidata.org/wiki/Special:EntityData/>",
			"PREFIX s: <http://www.wikidata.org/entity/statement/>",
			"PREFIX ref: <http://www.wikidata.org/reference/>",
			"PREFIX v: <http://www.wikidata.org/value/>",
			"PREFIX wdt: <http://www.wikidata.org/prop/direct/>",
			"PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/>",
			"PREFIX p: <http://www.wikidata.org/prop/>",
			"PREFIX ps: <http://www.wikidata.org/prop/statement/>",
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

	HybridStore hybridStore;
	final Set<LuceneSail> luceneSails = new HashSet<>();
	SailRepository repository;

	void clearHybridStore(String location) throws IOException {
		if (model.containsKey(location)) {
			logger.info("Clear old store");
			model.remove(location);
			repository.shutDown();
			hybridStore = null;
			repository = null;
		}
		FileUtils.deleteRecursively(Paths.get(locationNative));
	}

	void initializeHybridStore(String location) throws IOException {
		if (!model.containsKey(location)) {
			model.put(location, null);
			HDTSpecification spec = new HDTSpecification();
			spec.setOptions(hdtSpec);

			File hdtFile = new File(HybridStoreFiles.getHDTIndex(location, hdtIndexName));
			if (!hdtFile.exists()) {
				File tempRDF = new File(location + "tmp_index.nt");
				Files.createDirectories(tempRDF.getParentFile().toPath());
				Files.createFile(tempRDF.toPath());
				try {
					HDT hdt = HDTManager.generateHDT(tempRDF.getAbsolutePath(), "uri", RDFNotation.NTRIPLES, spec, null);
					hdt.saveToHDT(hdtFile.getPath(), null);
				} catch (ParserException e) {
					throw new IOException("Can't parse the RDF file", e);
				}
				Files.delete(Paths.get(tempRDF.getAbsolutePath()));
			}

			hybridStore = new HybridStore(locationHdt, hdtIndexName, spec, locationNative, false);
			hybridStore.setThreshold(threshold);
			logger.info("Threshold for triples in Native RDF store: " + threshold + " triples");

			SailCompiler compiler = new SailCompiler();
			compiler.registerDirObject(hybridStore.getHybridStoreFiles());
			LuceneSailCompiler luceneCompiler = (LuceneSailCompiler) compiler.getCompiler(SailCompilerSchema.LUCENE_TYPE);
			luceneCompiler.reset();
			InputStream stream;
			try {
				stream = new FileInputStream(repoModel);
			} catch (IOException e) {
				stream = getClass().getClassLoader().getResourceAsStream(repoModel);
				if (stream == null) {
					throw e;
				}
			}
			try (InputStream fstream = stream) {
				compiler.load(fstream, Rio.getParserFormatForFileName(repoModel).orElseThrow());
			}

			NotifyingSail source;

			IRI storageMode;
			try (SailCompiler.SailCompilerReader reader = compiler.getReader()) {
				storageMode = reader
						.searchOneOpt(SailCompilerSchema.MAIN, SailCompilerSchema.STORAGE_MODE)
						.map(SailCompiler::asIRI)
						.orElse(SailCompilerSchema.HYBRIDSTORE_STORAGE);
			}

			if (storageMode.equals(SailCompilerSchema.HYBRIDSTORE_STORAGE)) {
				source = hybridStore;
			} else if (storageMode.equals(SailCompilerSchema.NATIVESTORE_STORAGE)) {
				source = new NativeStore(new File(hybridStore.getHybridStoreFiles().getLocationNative(), "nativeglobal"));
			} else if (storageMode.equals(SailCompilerSchema.MEMORYSTORE_STORAGE)) {
				source = new MemoryStore();
			} else {
				throw new RuntimeException("Bad storage mode: " + storageMode);
			}

			repository = new SailRepository(compiler.compile(source));
			repository.init();

			luceneSails.clear();
			luceneSails.addAll(luceneCompiler.getSails());
		}
	}

	/**
	 * ask for a merge of the hybrid store
	 *
	 * @return see {@link com.the_qa_company.q_endpoint.hybridstore.HybridStore#mergeStore()} return value
	 * @throws IOException if the store can't be merged or init
	 */
	public MergeRequestResult askForAMerge() throws IOException {
		initializeHybridStore(locationHdt);
		this.hybridStore.mergeStore();
		return new MergeRequestResult(true);
	}

	/**
	 * @return if the store is merging
	 * @throws IOException if the store can't be merged or init
	 */
	public IsMergingResult isMerging() throws IOException {
		initializeHybridStore(locationHdt);
		return new IsMergingResult(hybridStore.isMergeTriggered);
	}

	public void execute(String sparqlQuery, int timeout, String acceptHeader, Consumer<String> mimeSetter, OutputStream out) throws IOException {
		initializeHybridStore(locationHdt);

		try (RepositoryConnection connection = repository.getConnection()) {
			sparqlQuery = sparqlQuery.replaceAll("MINUS \\{(.*\\n)+.+}\\n\\s+}", "");
			//sparqlQuery = sparqlPrefixes+sparqlQuery;

			logger.info("Running given sparql query: " + sparqlQuery);

			ParsedQuery parsedQuery =
					QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);

			if (parsedQuery instanceof ParsedTupleQuery) {
				TupleQuery query = connection.prepareTupleQuery(sparqlQuery);
				QueryResultFormat format = FormatUtils.getResultWriterFormat(acceptHeader)
						.orElseThrow(() -> new ServerWebInputException("accept formats not supported: " + acceptHeader));
				mimeSetter.accept(format.getDefaultMIMEType());
				TupleQueryResultWriter writer = TupleQueryResultWriterRegistry.getInstance()
						.get(format)
						.orElseThrow()
						.getWriter(out);
				query.setMaxExecutionTime(timeout);
				try {
					query.evaluate(writer);
				} catch (QueryEvaluationException q) {
					logger.error("This exception was caught [" + q + "]");
					q.printStackTrace();
					throw new RuntimeException(q);
				}
			} else if (parsedQuery instanceof ParsedBooleanQuery) {
				BooleanQuery query = connection.prepareBooleanQuery(sparqlQuery);
				QueryResultFormat format = FormatUtils.getResultWriterFormat(acceptHeader)
						.orElseThrow(() -> new ServerWebInputException("accept formats not supported: " + acceptHeader));
				mimeSetter.accept(format.getDefaultMIMEType());
				TupleQueryResultWriter writer = TupleQueryResultWriterRegistry.getInstance()
						.get(format)
						.orElseThrow()
						.getWriter(out);
				query.setMaxExecutionTime(timeout);
				writer.handleBoolean(query.evaluate());
				connection.close();
			} else if (parsedQuery instanceof ParsedGraphQuery) {
				GraphQuery query = connection.prepareGraphQuery(sparqlQuery);
				RDFFormat format = FormatUtils.getRDFWriterFormat(acceptHeader)
						.orElseThrow(() -> new ServerWebInputException("accept formats not supported: " + acceptHeader));
				mimeSetter.accept(format.getDefaultMIMEType());
				RDFHandler handler = Rio.createWriter(format, out);
				try {
					query.evaluate(handler);
				} catch (QueryEvaluationException q) {
					logger.error("This exception was caught [" + q + "]");
					q.printStackTrace();
					throw new RuntimeException(q);
				}
			} else {
				throw new ServerWebInputException("query not supported");
			}
		}
	}

	public void executeUpdate(String sparqlQuery, int timeout, OutputStream out) throws IOException {
		initializeHybridStore(locationHdt);
		//logger.info("Running update query:"+sparqlQuery);
		sparqlQuery = sparqlPrefixes + sparqlQuery;
		sparqlQuery = Pattern.compile("MINUS \\{(?s).*?}\\n {2}}").matcher(sparqlQuery).replaceAll("");
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.setParserConfig(new ParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false));

			Update preparedUpdate = connection.prepareUpdate(QueryLanguage.SPARQL, sparqlQuery);
			preparedUpdate.setMaxExecutionTime(timeout);

			Stopwatch stopwatch = Stopwatch.createStarted();
			preparedUpdate.execute();
			stopwatch.stop(); // optional
			logger.info("Time elapsed to execute update query: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
			try (JsonGenerator gen = Json.createGenerator(out)) {
				gen.writeStartObject().write("ok", true).writeEnd();
			}
		}
	}

	public LoadFileResult loadFile(InputStream input, String filename) throws IOException {
		String rdfInput = locationHdt + filename;
		String hdtOutput = HybridStoreFiles.getHDTIndex(locationHdt, hdtIndexName);
		String baseURI = "file://" + rdfInput;

		Files.createDirectories(Paths.get(locationHdt));
		Files.deleteIfExists(Paths.get(hdtOutput));
		Files.deleteIfExists(Paths.get(HybridStoreFiles.getHDTIndexV11(locationHdt, hdtIndexName)));

		HDTSpecification spec = new HDTSpecification();
		spec.setOptions(hdtSpec);

		compressToHdt(input, baseURI, filename, hdtOutput, spec);

		clearHybridStore(locationHdt);
		initializeHybridStore(locationHdt);
		try {
			for (LuceneSail sail : luceneSails) {
				sail.reindex();
			}
		} catch (Exception e) {
			throw new RuntimeException("Can't reindex the lucene sail(s)!", e);
		}
		return new LoadFileResult(true);
	}

	private void compressToHdt(InputStream inputStream, String baseURI, String filename, String hdtLocation, HDTSpecification specs) throws IOException {
		/* Maximum amount of memory the JVM will attempt to use */
		long maxMemory = Runtime.getRuntime().maxMemory();
		long chunkSize =
				(long) Math.floor((maxMemory - 1024 * 1024 * 1024) * 0.85)
				//128*1024
				;

		if (debugMaxChunkSize > 0) {
			assert debugMaxChunkSize < chunkSize : "debugMaxChunkSize can't be higher than chunkSize";
			chunkSize = debugMaxChunkSize;
		}
		logger.info("Maximal available memory {}", maxMemory);
		File hdtParentFile = new File(hdtLocation).getParentFile();
		String hdtParent = hdtParentFile.getAbsolutePath();
		Files.createDirectories(hdtParentFile.toPath());

		StopWatch timeWatch = new StopWatch();

		File tempFile = new File(filename);
		// the compression will not fit in memory, cat the files in chunks and use hdtCat

		// uncompress the file if required
		InputStream fileStream = RDFStreamUtils.uncompressedStream(inputStream, filename);
		// get a triple iterator for this stream
		Iterator<TripleString> tripleIterator = RDFStreamUtils.readRDFStreamAsTripleStringIterator(
				fileStream,
				Rio.getParserFormatForFileName(filename)
						.orElseThrow(() -> new ServerWebInputException("file format not supported " + filename)),
				true
		);
		// split this triple iterator to filed triple iterator
		FileTripleIterator it = new FileTripleIterator(tripleIterator, chunkSize);

		int file = 0;
		String lastFile = null;
		while (it.hasNewFile()) {
			logger.info("Compressing #" + file);
			String hdtOutput = new File(tempFile.getParent(), tempFile.getName() + "."
					+ String.format("%03d", file) + ".hdt").getAbsolutePath();
			try {
				HDT hdtDump = HDTManager.generateHDT(it, baseURI, specs, null);
				hdtDump.saveToHDT(hdtOutput, null);
				hdtDump.close();
			} catch (ParserException e) {
				throw new IOException("Can't parse the RDF file", e);
			}

			System.gc();
			logger.info("Competed into " + hdtOutput);
			if (file > 0) {
				// not the first file, so we have at least 2 files
				logger.info("Cat " + hdtOutput);
				String nextIndex = hdtParent + "/index_cat_tmp_" + file + ".hdt";
				HDT tmp = HDTManager.catHDT(nextIndex, lastFile, hdtOutput, specs, null);

				System.out.println("saving hdt with " + tmp.getTriples().getNumberOfElements() + " triple(s) into " + nextIndex);
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

}

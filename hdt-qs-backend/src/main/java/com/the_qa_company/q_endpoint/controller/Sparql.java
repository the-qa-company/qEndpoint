package com.the_qa_company.q_endpoint.controller;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.hybridstore.HybridStoreFiles;
import com.the_qa_company.q_endpoint.utils.FileTripleIterator;
import com.the_qa_company.q_endpoint.utils.RDFStreamUtils;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.parser.*;
import com.the_qa_company.q_endpoint.utils.FileUtils;
import com.the_qa_company.q_endpoint.utils.sail.builder.SailCompiler;
import com.the_qa_company.q_endpoint.utils.sail.builder.SailCompilerSchema;
import com.the_qa_company.q_endpoint.utils.sail.builder.compiler.LuceneSailCompiler;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.query.resultio.binary.BinaryQueryResultWriterFactory;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

	private static final Logger logger = LoggerFactory.getLogger(Sparql.class);
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

	public static int count = 0;
	public static int countEquals = 0;

    private String sparqlPrefixes = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
            "PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#>\n" +
            "PREFIX dct: <http://purl.org/dc/terms/>\n" +
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
            "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
            "PREFIX wikibase: <http://wikiba.se/ontology#>\n" +
            "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
            "PREFIX cc: <http://creativecommons.org/ns#>\n" +
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n" +
            "PREFIX prov: <http://www.w3.org/ns/prov#>\n" +
            "PREFIX wd: <http://www.wikidata.org/entity/>\n" +
            "PREFIX data: <https://www.wikidata.org/wiki/Special:EntityData/>\n" +
            "PREFIX s: <http://www.wikidata.org/entity/statement/>\n" +
            "PREFIX ref: <http://www.wikidata.org/reference/>\n" +
            "PREFIX v: <http://www.wikidata.org/value/>\n" +
            "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n" +
            "PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/>\n" +
            "PREFIX p: <http://www.wikidata.org/prop/>\n" +
            "PREFIX ps: <http://www.wikidata.org/prop/statement/>\n" +
            "PREFIX psv: <http://www.wikidata.org/prop/statement/value/>\n" +
            "PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>\n" +
            "PREFIX pq: <http://www.wikidata.org/prop/qualifier/>\n" +
            "PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>\n" +
            "PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>\n" +
            "PREFIX pr: <http://www.wikidata.org/prop/reference/>\n" +
            "PREFIX prv: <http://www.wikidata.org/prop/reference/value/>\n" +
            "PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/>\n" +
            "PREFIX wdno: <http://www.wikidata.org/prop/novalue/> \n";

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

	void initializeHybridStore(String location) throws Exception {
		if (!model.containsKey(location)) {
			model.put(location, null);
			HDTSpecification spec = new HDTSpecification();
			spec.setOptions(hdtSpec);

			File hdtFile = new File(HybridStoreFiles.getHDTIndex(location, hdtIndexName));
			if (!hdtFile.exists()) {
				File tempRDF = new File(location + "tmp_index.nt");
				if (!tempRDF.getParentFile().exists())
					tempRDF.getParentFile().mkdir();
				tempRDF.createNewFile();
				HDT hdt = HDTManager.generateHDT(tempRDF.getAbsolutePath(), "uri", RDFNotation.NTRIPLES, spec, null);
				hdt.saveToHDT(hdtFile.getPath(), null);
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

			repository = new SailRepository(compiler.compile(hybridStore));
			repository.init();

			luceneSails.clear();
			luceneSails.addAll(luceneCompiler.getSails());
		}
	}

	/**
	 * ask for a merge of the hybrid store
	 *
	 * @return see {@link com.the_qa_company.q_endpoint.hybridstore.HybridStore#mergeStore()} return value
	 * @throws Exception if the store can't be merged or init
	 */
	public MergeRequestResult askForAMerge() throws Exception {
		initializeHybridStore(locationHdt);
		this.hybridStore.mergeStore();
		return new MergeRequestResult(true);
	}

	public IsMergingResult isMerging() throws Exception {
		initializeHybridStore(locationHdt);
		return new IsMergingResult(hybridStore.isMergeTriggered);
	}

    public String executeJson(String sparqlQuery, int timeout) throws Exception {
        initializeHybridStore(locationHdt);


        RepositoryConnection connection = repository.getConnection();

        try {
            sparqlQuery = sparqlQuery.replaceAll("MINUS \\{(.*\\n)+.+}\\n\\s+\\}", "");
            //sparqlQuery = sparqlPrefixes+sparqlQuery;

            logger.info("Running given sparql query: " + sparqlQuery);

            ParsedQuery parsedQuery =
                    QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);

            if (parsedQuery instanceof ParsedTupleQuery) {
                TupleQuery query = connection.prepareTupleQuery(sparqlQuery);
//            Explanation explain = query.explain(Explanation.Level.Timed);
//            System.out.println(explain);

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                TupleQueryResultHandler writer = new SPARQLResultsJSONWriter(out);
                query.setMaxExecutionTime(timeout);
                try {
                    query.evaluate(writer);
                } catch (QueryEvaluationException q) {
                    logger.error("This exception was caught [" + q + "]");
                    q.printStackTrace();
                    return "{\"timeout\":" + timeout + "}";
                    //q.printStackTrace();
                } finally {
                    connection.close();
                }
                return new String(out.toByteArray());
            } else if (parsedQuery instanceof ParsedBooleanQuery) {
                BooleanQuery query = connection.prepareBooleanQuery(sparqlQuery);
                if (query.evaluate() == true) {
                    connection.close();
                    return "{ \"head\" : { } , \"boolean\" : true }";
                } else {
                    connection.close();
                    return "{ \"head\" : { } , \"boolean\" : false }";
                }

            } else {
                System.out.println("Not knowledge-base yet: query is neither a SELECT nor an ASK");
                return "Bad Request : query not supported ";
            }
        } finally {
            connection.close();
        }
    }

    public String executeXML(String sparqlQuery, int timeout) throws Exception {
        logger.info("Json " + sparqlQuery);
        initializeHybridStore(locationHdt);

        ParsedQuery parsedQuery =
                QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);


        RepositoryConnection connection = repository.getConnection();
        try {
            if (parsedQuery instanceof ParsedTupleQuery) {
                TupleQuery query = connection.prepareTupleQuery(sparqlQuery);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                TupleQueryResultHandler writer = new SPARQLResultsXMLWriter(out);
                query.setMaxExecutionTime(timeout);
                try {
                    query.evaluate(writer);
                } catch (QueryEvaluationException q) {
                    logger.error("This exception was caught [" + q + "]");
                } finally {
                    connection.close();
                }
                return out.toString("UTF8");
            } else if (parsedQuery instanceof ParsedBooleanQuery) {
                BooleanQuery query = connection.prepareBooleanQuery(sparqlQuery);
                if (query.evaluate() == true) {
                    connection.close();
                    return "{ \"head\" : { } , \"boolean\" : true }";
                } else {
                    connection.close();
                    return "{ \"head\" : { } , \"boolean\" : false }";
                }
            } else {
                System.out.println("Not knowledge-base yet: query is neither a SELECT nor an ASK");
                return "Bad Request : query not supported ";
            }
        } finally {
            connection.close();
        }
    }

    public String executeBinary(String sparqlQuery, int timeout) throws Exception {
        initializeHybridStore(locationHdt);

        sparqlQuery = sparqlPrefixes + sparqlQuery;

        //logger.info("Json " + sparqlQuery);

        ParsedQuery parsedQuery =
                QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);
        RepositoryConnection connection = repository.getConnection();
        try {
            if (parsedQuery instanceof ParsedTupleQuery) {

                TupleQuery query = connection.prepareTupleQuery(sparqlQuery);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                TupleQueryResultHandler writer = new BinaryQueryResultWriterFactory().getWriter(out);
                query.setMaxExecutionTime(timeout);

                Stopwatch stopwatch = Stopwatch.createStarted();
                try {
                    query.evaluate(writer);
                } catch (QueryEvaluationException q) {
                    logger.error("This exception was caught [" + q + "]");
                } finally {
                    connection.close();
                }
                stopwatch.stop(); // optional
                logger.info("Time elapsed to execute tuple query: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

                return out.toString("UTF8");
            } else if (parsedQuery instanceof ParsedBooleanQuery) {
                BooleanQuery query = connection.prepareBooleanQuery(sparqlQuery);
                if (query.evaluate() == true) {
                    connection.close();
                    return "{ \"head\" : { } , \"boolean\" : true }";
                } else {
                    connection.close();
                    return "{ \"head\" : { } , \"boolean\" : false }";
                }
            } else {
                System.out.println("Not knowledge-base yet: query is neither a SELECT nor an ASK");
                return "Bad Request : query not supported ";
            }
        } finally {
            connection.close();
        }
    }

    public String executeUpdate(String sparqlQuery, int timeout) throws Exception {
        initializeHybridStore(locationHdt);
        //logger.info("Running update query:"+sparqlQuery);
        sparqlQuery = sparqlPrefixes + sparqlQuery;
        sparqlQuery = Pattern.compile("MINUS \\{(?s).*?}\\n {2}}").matcher(sparqlQuery).replaceAll("");
        SailRepositoryConnection connection = repository.getConnection();
        try {
            connection.setParserConfig(new ParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false));

            Update preparedUpdate = connection.prepareUpdate(QueryLanguage.SPARQL, sparqlQuery);
            preparedUpdate.setMaxExecutionTime(timeout);

            if (preparedUpdate != null) {
                Stopwatch stopwatch = Stopwatch.createStarted();
                preparedUpdate.execute();
                stopwatch.stop(); // optional
                logger.info("Time elapsed to execute update query: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
                connection.close();
                return "OK";
            }
            return null;
        } finally {
            connection.close();
        }
    }

    public String executeTurtle(String sparqlQuery, int timeout) throws Exception {
        logger.info("Turtle " + sparqlQuery);
        System.out.println("TURTLE !!!!!!!!!!!!");
        initializeHybridStore(locationHdt);
        ParsedQuery parsedQuery =
                QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);
        System.out.println(parsedQuery.getClass());
        RepositoryConnection connection = repository.getConnection();
        try {
            if (parsedQuery instanceof ParsedGraphQuery) {
                GraphQuery query = connection.prepareGraphQuery(sparqlQuery);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                RDFHandler turtleWriter = Rio.createWriter(RDFFormat.TURTLE, out);
                query.evaluate(turtleWriter);
                query.setMaxExecutionTime(timeout);
                connection.close();
                return out.toString("UTF8");
            } else {
                System.out.println("Not knowledgebase yet: query is not construct");
            }
            return null;
        } finally {
            connection.close();
        }
    }

    public ResponseEntity<String> loadFile(InputStream input, String filename) {
        String rdfInput = locationHdt + filename;
        String hdtOutput = HybridStoreFiles.getHDTIndex(locationHdt, hdtIndexName);
        String baseURI = "file://" + rdfInput;
        try {
            new File(locationHdt).mkdirs();
            Files.deleteIfExists(Paths.get(hdtOutput));
            Files.deleteIfExists(Paths.get(HybridStoreFiles.getHDTIndexV11(locationHdt, hdtIndexName)));

            HDTSpecification spec = new HDTSpecification();
            spec.setOptions(hdtSpec);

            RDFNotation notation = RDFNotation.guess(filename);
            if (notation == RDFNotation.NTRIPLES) {
                compressToHdt(input, baseURI, filename, hdtOutput, spec);
            } else {
                Files.copy(input, Paths.get(locationHdt + filename), StandardCopyOption.REPLACE_EXISTING);
                HDT hdt = HDTManager.generateHDT(rdfInput, baseURI, notation, spec, null);
                hdt.saveToHDT(hdtOutput, null);
            }

            clearHybridStore(locationHdt);
            initializeHybridStore(locationHdt);
            for (LuceneSail sail : luceneSails) {
                sail.reindex();
            }
            return ResponseEntity.status(HttpStatus.OK).body("File was loaded successfully...\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("File was not loaded...\n");
    }

    private void compressToHdt(InputStream inputStream, String baseURI, String filename, String hdtLocation, HDTSpecification specs) throws IOException, ParserException {
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
        hdtParentFile.mkdirs();

        StopWatch timeWatch = new StopWatch();

        File tempFile = new File(filename);
        // the compression will not fit in memory, cat the files in chunks and use hdtCat

        // uncompress the file if required
        InputStream fileStream = RDFStreamUtils.uncompressedStream(inputStream, filename);
        // get a triple iterator for this stream
        Iterator<TripleString> tripleIterator = RDFStreamUtils.readRDFStreamAsTripleStringIterator(fileStream, RDFFormat.NTRIPLES, true);
        // split this triple iterator to filed triple iterator
        FileTripleIterator it = new FileTripleIterator(tripleIterator, chunkSize);

        int file = 0;
        String lastFile = null;
        while (it.hasNewFile()) {
            logger.info("Compressing #" + file);
            HDT hdtDump = HDTManager.generateHDT(it, baseURI, specs, null);
            String hdtOutput = new File(tempFile.getParent(), tempFile.getName() + "."
                    + String.format("%03d", file) + ".hdt").getAbsolutePath();
            hdtDump.saveToHDT(hdtOutput, null);
            hdtDump.close();
            hdtDump = null;
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
                tmp = null;
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

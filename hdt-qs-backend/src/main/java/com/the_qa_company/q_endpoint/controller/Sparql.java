package com.the_qa_company.q_endpoint.controller;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import com.the_qa_company.q_endpoint.hybridstore.HybridStore;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.explanation.Explanation;
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
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Component
public class Sparql {
    private static final Logger logger = LoggerFactory.getLogger(Sparql.class);
    private final HashMap<String, RepositoryConnection> model = new HashMap<>();

    private NativeStore nativeStore;
    @Value("${locationHdt}")
    private String locationHdt;

    @Value("${locationNative}")
    private String locationNative;

    @Value("${threshold}")
    private int threshold;

    @Value("${hdtSpecification}")
    private String hdtSpec;

    private String hdtindex = "index.hdt";

    private HybridStore hybridStore;
    private LuceneSail luceneSail;
    private SailRepository repository;

    public static int count = 0 ;
    public static int countEquals = 0 ;

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
    void initializeHybridStore(String location) throws Exception {
        if (!model.containsKey(location)) {
            model.put(location, null);
            HDTSpecification spec = new HDTSpecification();
            spec.setOptions(hdtSpec);

            File hdtFile = new File(location+"index.hdt");
            if(!hdtFile.exists()){
                File tempRDF = new File(location+"tmp_index.nt");
                if(!tempRDF.getParentFile().exists())
                    tempRDF.getParentFile().mkdir();
                tempRDF.createNewFile();
                HDT hdt = HDTManager.generateHDT(tempRDF.getAbsolutePath(),"uri", RDFNotation.NTRIPLES,spec,null);
                hdt.saveToHDT(hdtFile.getPath(),null);
                Files.delete(Paths.get(tempRDF.getAbsolutePath()));
            }

            hybridStore = new HybridStore(locationHdt,spec,locationNative,false);
            hybridStore.setThreshold(threshold);
            logger.info("Threshold for triples in Native RDF store: "+threshold+" triples");
            luceneSail = new LuceneSail();
            luceneSail.setReindexQuery("select ?s ?p ?o where {?s ?p ?o}");
            luceneSail.setParameter(LuceneSail.LUCENE_DIR_KEY, location + "/lucene");
            luceneSail.setParameter(LuceneSail.WKT_FIELDS, "http://nuts.de/geometry");
            luceneSail.setBaseSail(hybridStore);
            luceneSail.setEvaluationMode(TupleFunctionEvaluationMode.NATIVE);
            luceneSail.initialize();
            repository = new SailRepository(luceneSail);
            repository.init();
            //lucenesail.reindex();
        }
    }
    public String executeJson(String sparqlQuery, int timeout) throws Exception {
        initializeHybridStore(locationHdt);



        RepositoryConnection connection = repository.getConnection();

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
            } catch (QueryEvaluationException q){
                logger.error("This exception was caught ["+q+"]");
                q.printStackTrace();
                return "{\"timeout\":"+timeout+"}";
                //q.printStackTrace();
            }
            finally {
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
    }
    public String executeXML(String sparqlQuery, int timeout) throws Exception {
        logger.info("Json " + sparqlQuery);
        initializeHybridStore(locationHdt);

        ParsedQuery parsedQuery =
                QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);


        RepositoryConnection connection = repository.getConnection();
        if (parsedQuery instanceof ParsedTupleQuery) {
            TupleQuery query = connection.prepareTupleQuery(sparqlQuery);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            TupleQueryResultHandler writer = new SPARQLResultsXMLWriter(out);
            query.setMaxExecutionTime(timeout);
            try {
                query.evaluate(writer);
            } catch (QueryEvaluationException q){
                logger.error("This exception was caught ["+q+"]");
            }finally {
                connection.close();
            }
            return out.toString("UTF8");
        } else if (parsedQuery instanceof ParsedBooleanQuery) {
            BooleanQuery query = model.get(locationHdt).prepareBooleanQuery(sparqlQuery);
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
    }
    public String executeBinary(String sparqlQuery, int timeout) throws Exception {
        initializeHybridStore(locationHdt);

        sparqlQuery = sparqlPrefixes+sparqlQuery;

        //logger.info("Json " + sparqlQuery);

        ParsedQuery parsedQuery =
                QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);
        RepositoryConnection connection = repository.getConnection();
        if (parsedQuery instanceof ParsedTupleQuery) {

            TupleQuery query = connection.prepareTupleQuery(sparqlQuery);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            TupleQueryResultHandler writer = new BinaryQueryResultWriterFactory().getWriter(out);
            query.setMaxExecutionTime(timeout);

            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                query.evaluate(writer);
            } catch (QueryEvaluationException q){
                logger.error("This exception was caught ["+q+"]");
            }finally {
                connection.close();
            }
            stopwatch.stop(); // optional
            logger.info("Time elapsed to execute tuple query: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return out.toString("UTF8");
        } else if (parsedQuery instanceof ParsedBooleanQuery) {
            BooleanQuery query = model.get(locationHdt).prepareBooleanQuery(sparqlQuery);
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
    }
    public int getCurrentCount() throws Exception {
        initializeHybridStore(locationHdt);
        String queryCount = "select (count(*) as ?c) where { ?s ?p ?o}";

        RepositoryConnection connection = repository.getConnection();
        TupleQuery tupleQuery = connection.prepareTupleQuery(queryCount);
        try (TupleQueryResult result = tupleQuery.evaluate()) {
            if(result.hasNext()) {
                BindingSet bindingSet = result.next();
                org.eclipse.rdf4j.model.Value valueOfC = bindingSet.getValue("c");
                connection.close();
                return Integer.parseInt(valueOfC.stringValue());
            }
        }
        return 0;
    }
    public String executeUpdate(String sparqlQuery, int timeout) throws Exception {
        initializeHybridStore(locationHdt);
        //logger.info("Running update query:"+sparqlQuery);
        sparqlQuery = sparqlPrefixes + sparqlQuery;
        sparqlQuery = Pattern.compile("MINUS \\{(?s).*?}\\n {2}}").matcher(sparqlQuery).replaceAll("");
        SailRepositoryConnection connection = repository.getConnection();
        connection.setParserConfig(new ParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false));

        Update preparedUpdate = connection.prepareUpdate(QueryLanguage.SPARQL,sparqlQuery);
         preparedUpdate.setMaxExecutionTime(timeout);

//        ValueFactory valueFactory = connection.getValueFactory();
//        SPARQLParser parser = new SPARQLParser();
//        ParsedUpdate parsedUpdate = parser.parseUpdate(sparqlQuery,(String)null,valueFactory);
//        Update preparedUpdate = new SailUpdate(parsedUpdate,connection);
        if(preparedUpdate != null) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            preparedUpdate.execute();
            stopwatch.stop(); // optional
            logger.info("Time elapsed to execute update query: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
            connection.close();
            return "OK";
        }
        return null;
    }
    public String executeTurtle(String sparqlQuery, int timeout) throws Exception {
        logger.info("Turtle " + sparqlQuery);
        System.out.println("TURTLE !!!!!!!!!!!!");
        initializeHybridStore(locationHdt);
        ParsedQuery parsedQuery =
                QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);
        System.out.println(parsedQuery.getClass());
        RepositoryConnection connection = repository.getConnection();
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
    }
    public String loadFile(InputStream input, String filename){
        try {
            Files.deleteIfExists(Paths.get(locationHdt+"index.hdt"));
            Files.deleteIfExists(Paths.get(locationHdt+"index.hdt.index.v1-1"));

            String rdfInput = locationHdt+filename;
            String hdtOutput = locationHdt+"index.hdt";

            Files.copy(input, Paths.get(locationHdt+filename), StandardCopyOption.REPLACE_EXISTING);
            RDFNotation notation = RDFNotation.guess(rdfInput);
            String baseURI = "file://"+rdfInput;
            HDTSpecification spec = new HDTSpecification();
            spec.setOptions(hdtSpec);
            HDT hdt = HDTManager.generateHDT(rdfInput, baseURI,notation , spec, null);
            hdt.saveToHDT(hdtOutput,null);
            initializeHybridStore(locationHdt);
            return "File was loaded successfully...\n";
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "error";
    }
    public void clearAllData(){
        try {
            initializeHybridStore(locationHdt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        repository.getConnection().clear((Resource)null);
        try {
            Files.deleteIfExists(Paths.get(locationHdt+"index.hdt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

package eu.qanswer.enpoint;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.explanation.Explanation;
import org.eclipse.rdf4j.query.parser.*;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.HDTLuceneSail;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf4j.HybridStore;
import org.rdfhdt.hdt.rdf4j.extensible.MyStore;
import org.rdfhdt.hdt.rdf4j.misc.HDTStore;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

@Component
public class Sparql {
    private static final Logger logger = LoggerFactory.getLogger(Sparql.class);
    private final HashMap<String, RepositoryConnection> model = new HashMap<>();

    private final int THRESHOLD = 2;
    private NativeStore nativeStore;
    @Value("${locationHdt}")
    private String locationHdt;
    @Value("${locationNativeA}")
    private String locationNativeA;
    @Value("${locationNativeB}")
    private String locationNativeB;

    @Value("${locationDelete}")
    private String locationDelete;

    private String hdtindex = "index.hdt";

    private NativeStore nativeStoreA;
    private NativeStore nativeStoreB;
    private NativeStore deleteStore;

    private HybridStore hybridStore;
    private LuceneSail luceneSail;
    private SailRepository repository;
    void initializeHybridStore(String location) throws Exception {
        if (!model.containsKey(location)) {
            model.put(location, null);
            HDTSpecification spec = new HDTSpecification();
            //spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");

            HDT hdt = HDTManager.mapIndexedHDT(
                            new File(location+"index.hdt").getAbsolutePath(),spec);

            File dataDir1 = new File(locationNativeA);
            File dataDir2 = new File(locationNativeB);
            File dataDir3 = new File(locationDelete);

            String indexes = "spoc,posc,cosp";
            nativeStoreA = new NativeStore(dataDir1,indexes);
            nativeStoreB = new NativeStore(dataDir2,indexes);
            deleteStore = new NativeStore(dataDir3,indexes);
            hybridStore = new HybridStore(this.nativeStoreA,this.nativeStoreB,deleteStore,hdt,locationHdt,2);
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
        logger.info("Json " + sparqlQuery);
        initializeHybridStore(locationHdt);

        ParsedQuery parsedQuery =
                QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);


        RepositoryConnection connection = repository.getConnection();
        if (parsedQuery instanceof ParsedTupleQuery) {
            TupleQuery query = connection.prepareTupleQuery(sparqlQuery);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            TupleQueryResultHandler writer = new SPARQLResultsJSONWriter(out);
            query.setMaxExecutionTime(timeout);
            try {
                query.evaluate(writer);
            } catch (QueryEvaluationException q){
                logger.error("This exception was caught ["+q+"]");
            }
            return out.toString("UTF8");
        } else if (parsedQuery instanceof ParsedBooleanQuery) {
            BooleanQuery query = model.get(locationHdt).prepareBooleanQuery(sparqlQuery);
            if (query.evaluate() == true) {
                return "{ \"head\" : { } , \"boolean\" : true }";
            } else {
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
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                org.eclipse.rdf4j.model.Value valueOfC = bindingSet.getValue("c");
                return Integer.parseInt(valueOfC.stringValue());
            }
        }
        return 0;
    }
    public String executeUpdate(String sparqlQuery, int timeout) throws Exception {
        initializeHybridStore(locationHdt);
        logger.info("Running update query:"+sparqlQuery);
        RepositoryConnection connection = repository.getConnection();
        Update preparedUpdate = connection.prepareUpdate(QueryLanguage.SPARQL,sparqlQuery);
        if(preparedUpdate != null) {
            preparedUpdate.execute();
            return "OK\n";
        }
        return null;
    }
}

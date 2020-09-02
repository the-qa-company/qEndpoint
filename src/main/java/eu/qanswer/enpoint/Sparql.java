package eu.qanswer.enpoint;

import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.lucene.HDTLuceneSail;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.rdf4j.HDTSail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class Sparql {
    private static final Logger logger = LoggerFactory.getLogger(Sparql.class);
    private final HashMap<String, RepositoryConnection> model = new HashMap<>();

    @Value("${location}")
    private String location;

    void inizialize(String location) throws Exception {
        if (!model.containsKey(location)) {
            model.put(location, null);
            System.out.println("initialize "+location);
            HDT hdt =
                    HDTManager.mapIndexedHDT(
                            new File(location+"index_big.hdt").getAbsolutePath());
            HDTSail baseSail = new HDTSail(hdt);
            baseSail.initialize();
            HDTLuceneSail lucenesail = new HDTLuceneSail(baseSail);
            lucenesail.setReindexQuery(
                    "SELECT ?s ?p ?o WHERE { {SELECT ?s ?p ?o WHERE {?s ?p ?o . FILTER (?p=<https://linkedopendata.eu/prop/direct/P836> && lang(?o)=\"en\")} } UNION {SELECT ?s ?p ?o WHERE {?s ?p ?o . ?s <https://linkedopendata.eu/prop/direct/P35> <https://linkedopendata.eu/entity/Q196899> . FILTER (?p=<http://www.w3.org/2000/01/rdf-schema#label>)} } UNION {SELECT ?s ?p ?o WHERE {?s ?p ?o . FILTER (?p = <https://linkedopendata.eu/prop/direct/P127>) } } } order by ?s");
            lucenesail.setParameter(
                    LuceneSail.LUCENE_DIR_KEY, location + "lucene");
            lucenesail.setParameter(LuceneSail.WKT_FIELDS, "http://www.opengis.net/ont/geosparql#wktLiteral https://linkedopendata.eu/prop/direct/P127");
            lucenesail.setParameter(LuceneSail.MAX_DOCUMENTS_KEY, "5000");
            lucenesail.setBaseSail(baseSail);
            lucenesail.initialize();
            lucenesail.reindex();
            Repository db = new SailRepository(lucenesail);
            db.init();
            RepositoryConnection conn = db.getConnection();
            model.put(location, conn);
        }
    }

    public String executeJson(String sparqlQuery, int timeout)
            throws Exception {
        logger.info("Json " + sparqlQuery);
        inizialize(location);
        ExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        Callable<Object> task =
                new Callable<Object>() {
                    public Object call() throws Exception {
                        ParsedQuery parsedQuery =
                                QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);
                        if (parsedQuery instanceof ParsedTupleQuery) {
                            TupleQuery query = model.get(location).prepareTupleQuery(sparqlQuery);
                            ByteArrayOutputStream out = new ByteArrayOutputStream();
                            TupleQueryResultHandler writer = new SPARQLResultsJSONWriter(out);
                            query.evaluate(writer);
                            return out.toString("UTF8");
                        } else if (parsedQuery instanceof ParsedBooleanQuery) {
                            BooleanQuery query = model.get(location).prepareBooleanQuery(sparqlQuery);
                            if (query.evaluate() == true) {
                                return "{ \"head\" : { } , \"boolean\" : true }";
                            } else {
                                return "{ \"head\" : { } , \"boolean\" : false }";
                            }
                        } else {
                            System.out.println("Not knowledgebase yet: query is neither a SELECT nor an ASK");
                        }
                        return null;
                    };
                };
        Future<Object> future = executor.submit(task);
        Object result = null;
        try {
            result = future.get(timeout, TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
            logger.info(
                    "The query "
                            + sparqlQuery
                            + " timeed out!"+" Limit "+timeout);
        } catch (InterruptedException e) {
            e.printStackTrace();
            // handle the interrupts
        } finally {
            future.cancel(true);
            executor.shutdownNow(); // may or may not desire this
        }
        return (String) result;
    }
}

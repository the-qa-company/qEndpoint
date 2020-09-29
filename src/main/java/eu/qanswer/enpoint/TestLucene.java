package eu.qanswer.enpoint;


import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.lucene.LuceneIndex;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.lucene.LuceneSailSchema;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import java.io.File;
import java.io.FileInputStream;

/**
 * Example code showing how to use the LuceneSail
 *
 * @author sauermann
 */
public class TestLucene {

    /**
     * Create a lucene sail and use it
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        createSimple();
    }

    /**
     * Create a LuceneSail and add some triples to it, ask a query.
     */
    public static void createSimple() throws Exception {
        // create a sesame memory sail
        MemoryStore memoryStore = new MemoryStore();

        // create a lucenesail to wrap the memorystore
        LuceneSail lucenesail = new LuceneSail();
        lucenesail.setParameter(LuceneSail.INDEX_CLASS_KEY, LuceneIndex.class.getName());
        // set this parameter to let the lucene index store its data in ram
        lucenesail.setParameter(LuceneSail.LUCENE_RAMDIR_KEY, "true");
        lucenesail.setParameter(LuceneSail.WKT_FIELDS,"http://nuts.de/geometry");
        // set this parameter to store the lucene index on disk
        // lucenesail.setParameter(LuceneSail.LUCENE_DIR_KEY,
        // "./data/mydirectory");

        // wrap memorystore in a lucenesail
        lucenesail.setBaseSail(memoryStore);

        // create a Repository to access the sails
        SailRepository repository = new SailRepository(lucenesail);
        repository.initialize();

        try ( // add some test data, the FOAF ont
              SailRepositoryConnection connection = repository.getConnection()) {
            connection.begin();
            connection.add(new FileInputStream(new File("/home/alyhdr/Desktop/qa-company/example_hdt/index_big.nt")),
                    "", RDFFormat.NTRIPLES);
            connection.commit();

            // search for resources that mention "person"
            //String queryString = "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> PREFIX geo: <http://www.opengis.net/ont/geosparql#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> SELECT ?id  WHERE {  ?s <http://nuts.de/geometry> ?o . FILTER (geof:sfWithin(\"Point(-2.7633 47.826)\"^^geo:wktLiteral,?o)) }";
            String queryString = "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> PREFIX geo: <http://www.opengis.net/ont/geosparql#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> SELECT ?id  WHERE { ?s <http://nuts.de/geometry> ?o . FILTER (geof:sfWithin(\"POINT(33.30260 38.675310)\"^^geo:wktLiteral,?o)) ?s <http://example.com/id> ?id . }";
            tupleQuery(queryString, connection);
        } finally {
            repository.shutDown();
        }
    }

    private static void tupleQuery(String queryString, RepositoryConnection connection)
            throws QueryEvaluationException, RepositoryException, MalformedQueryException {
        System.out.println("Running query: \n" + queryString);
        TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        try (TupleQueryResult result = query.evaluate()) {
            // print the results
            System.out.println("Query results:");
            while (result.hasNext()) {
                BindingSet bindings = result.next();
                System.out.println("found match: ");
                for (Binding binding : bindings) {
                    System.out.println("\t" + binding.getName() + ": " + binding.getValue());
                }
            }
        }
    }

    private static void graphQuery(String queryString, RepositoryConnection connection)
            throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        System.out.println("Running query: \n" + queryString);
        GraphQuery query = connection.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
        try (GraphQueryResult result = query.evaluate()) {
            // print the results
            while (result.hasNext()) {
                Statement stmt = result.next();
                System.out.println("found match: " + stmt.getSubject().stringValue() + "\t"
                        + stmt.getPredicate().stringValue() + "\t" + stmt.getObject().stringValue());
            }
        }

    }
}

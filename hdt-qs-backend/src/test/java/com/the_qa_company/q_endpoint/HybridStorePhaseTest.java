package com.the_qa_company.q_endpoint;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@SpringBootTest(classes = HybridStorePhaseTest.class)
public class HybridStorePhaseTest {

    private static final Logger logger = LoggerFactory.getLogger(HybridStorePhaseTest.class);

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    HybridStore store;

    @Before
    public void setUp() throws IOException {
        HDTSpecification spec = new HDTSpecification();
        spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
        logger.info("Initialize the store ... ");
        File nativeStore = new File("./tests/native-store/");
        FileSystemUtils.deleteRecursively(nativeStore);
        nativeStore.mkdirs();
        File hdtStore = new File("./tests/hdt-store/");
        FileSystemUtils.deleteRecursively(hdtStore);
        hdtStore.mkdirs();
        File tmp = new File("./tests/hdt-store/temp.nt");
        tmp.delete();
        tmp.createNewFile();

        HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex("/Users/Dennis/Downloads/test/hdt-store/temp.nt", true, false);
        assert hdt != null;
        hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
        store = new HybridStore(
                hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
        );
    }

    @Test
    public void testBeforeMerge() throws IOException, NotFoundException, InterruptedException {

        int threshold = 1000;
        logger.info("Setting the threshold to "+threshold);
        store.setThreshold(threshold);
        SailRepository hybridStore = new SailRepository(store);

        logger.info("Insert some data");
        int numbeOfTriples = 500;
        String sparqlQuery = "INSERT DATA { ";
        for (int i = 0; i < numbeOfTriples; i++) {
            sparqlQuery += "	<http://s" + i + ">  <http://p" + i + ">  <http://o" + i + "> . ";
        }
        sparqlQuery += "} ";
        RepositoryConnection connection = hybridStore.getConnection();
        Update tupleQuery = connection.prepareUpdate(sparqlQuery);
        tupleQuery.execute();
        connection.commit();

        logger.info("QUERY 1");
        for (int i = 0; i < numbeOfTriples; i++) {
            sparqlQuery = "SELECT ?s WHERE { ?s  <http://p" + i + ">  <http://o" + i + "> . } ";
            TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
            TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();
            assertTrue(tupleQueryResult.hasNext());
            while (tupleQueryResult.hasNext()) {
                BindingSet b = tupleQueryResult.next();
                assertEquals("http://s" + i, b.getBinding("s").getValue().toString());
            }
        }
        logger.info("QUERY 2");
        for (int i = 0; i < numbeOfTriples; i++) {
            sparqlQuery = "SELECT ?p WHERE { <http://s" + i + ">  ?p  <http://o" + i + "> . } ";
            TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
            TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();
            assertEquals(true, tupleQueryResult.hasNext());
            while (tupleQueryResult.hasNext()) {
                BindingSet b = tupleQueryResult.next();
                assertEquals("http://p" + i, b.getBinding("p").getValue().toString());
            }
        }
        logger.info("DELETE");
        sparqlQuery = "DELETE DATA { ";
        for (int i = 0; i < 10; i++) {
            sparqlQuery += "	<http://s" + i + ">  <http://p" + i + ">  <http://o" + i + "> . ";
        }
        sparqlQuery += "} ";
        tupleQuery = connection.prepareUpdate(sparqlQuery);
        tupleQuery.execute();
        connection.commit();

        logger.info("QUERY");
        for (int i = 10; i < numbeOfTriples; i++) {
            sparqlQuery = "SELECT ?s WHERE { ?s  <http://p" + i + ">  <http://o" + i + "> . } ";
            TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
            TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();
            assertEquals(true, tupleQueryResult.hasNext());
            while (tupleQueryResult.hasNext()) {
                BindingSet b = tupleQueryResult.next();
                assertEquals("http://s" + i, b.getBinding("s").getValue().toString());
            }
        }

        logger.info("QUERY");
        for (int i = 10; i < numbeOfTriples; i++) {
            sparqlQuery = "SELECT ?p WHERE { <http://s" + i + ">  ?p  <http://o" + i + "> . } ";
            TupleQuery tupleQuery2 = connection.prepareTupleQuery(sparqlQuery);
            TupleQueryResult tupleQueryResult2 = tupleQuery2.evaluate();
            assertEquals(true, tupleQueryResult2.hasNext());
            while (tupleQueryResult2.hasNext()) {
                BindingSet b = tupleQueryResult2.next();
                assertEquals("http://p" + i, b.getBinding("p").getValue().toString());
            }
        }
        connection.close();
        hybridStore.shutDown();
    }

    @Test
    public void duringMerge(){

        int threshold = 400;
        logger.info("Setting the threshold to "+threshold);
        store.setThreshold(threshold);
        SailRepository hybridStore = new SailRepository(store);

        logger.info("Insert some data");
        int numbeOfTriples = 500;
        String sparqlQuery = "INSERT DATA { ";
        for (int i = 0; i < numbeOfTriples; i++) {
            sparqlQuery += "	<http://s" + i + ">  <http://p" + i + ">  <http://o" + i + "> . ";
        }
        sparqlQuery += "} ";
        RepositoryConnection connection = hybridStore.getConnection();
        Update tupleQuery = connection.prepareUpdate(sparqlQuery);
        tupleQuery.execute();
        connection.commit();



        // START MERGE
        // artificially rise the time to marge to 5 seconds
        store.setExtendsTimeMerge(15);
        sparqlQuery = "INSERT DATA { <http://s130>  <http://p130>  <http://o130> . } ";
        tupleQuery = connection.prepareUpdate(sparqlQuery);
        tupleQuery.execute();
        assertEquals(true,store.isMerging());

        logger.info("QUERY 1");
        for (int i = 0; i < numbeOfTriples; i++) {
            sparqlQuery = "SELECT ?s WHERE { ?s  <http://p" + i + ">  <http://o" + i + "> . } ";
            TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
            TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();
            assertTrue(tupleQueryResult.hasNext());
            while (tupleQueryResult.hasNext()) {
                BindingSet b = tupleQueryResult.next();
                assertEquals("http://s" + i, b.getBinding("s").getValue().toString());
            }
        }

        sparqlQuery = "INSERT DATA { <http://s600>  <http://p600>  <http://o600> . } ";
        tupleQuery = connection.prepareUpdate(sparqlQuery);
        tupleQuery.execute();

        logger.info("QUERY 1");

        sparqlQuery = "SELECT ?s WHERE { ?s  <http://p600>  <http://o600> . } ";
        TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
        TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();
        assertTrue(tupleQueryResult.hasNext());
        while (tupleQueryResult.hasNext()) {
            BindingSet b = tupleQueryResult.next();
            assertEquals("http://s600", b.getBinding("s").getValue().toString());
        }






        hybridStore.shutDown();

        //store.makeMerge();

        // convert delta during merge

        // after merge
    }
}

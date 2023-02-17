package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EndpointStorePhaseTest {
	private static final Logger logger = LoggerFactory.getLogger(EndpointStorePhaseTest.class);

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();
	EndpointStore store;

	@Before
	public void setUp() throws IOException {
		MergeRunnableStopPoint.debug = true;
		HDTOptions spec = HDTOptions.of();
		// spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
		logger.info("Initialize the store ... ");
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store/");
		File tmp = new File(hdtStore, "temp.nt");
		try (HDT hdt = Utility.createTempHdtIndex(tmp.getAbsolutePath(), true, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);
		}
		store = new EndpointStore(hdtStore.getAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + "/", false);
	}

	@After
	public void complete() {
		MergeRunnableStopPoint.debug = false;
	}

	@Test
	public void testBeforeMerge() {

		int threshold = 1000;
		logger.info("Setting the threshold to " + threshold);
		store.setThreshold(threshold);
		SailRepository endpointStore = new SailRepository(store);

		logger.info("Insert some data");
		int numbeOfTriples = 500;
		StringBuilder sparqlQuery = new StringBuilder("INSERT DATA { ");
		for (int i = 0; i < numbeOfTriples; i++) {
			sparqlQuery.append("	<http://s").append(i).append(">  <http://p").append(i).append(">  <http://o")
					.append(i).append("> . ");
		}
		sparqlQuery.append("} ");
		RepositoryConnection connection = endpointStore.getConnection();
		Update tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();
		connection.commit();

		logger.info("QUERY 1");
		for (int i = 0; i < numbeOfTriples; i++) {
			sparqlQuery = new StringBuilder("SELECT ?s WHERE { ?s  <http://p" + i + ">  <http://o" + i + "> . } ");
			TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
			try (TupleQueryResult tupleQueryResult = tupleQuery1.evaluate()) {
				assertTrue(tupleQueryResult.hasNext());
				while (tupleQueryResult.hasNext()) {
					BindingSet b = tupleQueryResult.next();
					assertEquals("http://s" + i, b.getBinding("s").getValue().toString());
				}
			}
		}
		logger.info("QUERY 2");
		for (int i = 0; i < numbeOfTriples; i++) {
			sparqlQuery = new StringBuilder("SELECT ?p WHERE { <http://s" + i + ">  ?p  <http://o" + i + "> . } ");
			TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
			try (TupleQueryResult tupleQueryResult = tupleQuery1.evaluate()) {
				assertTrue(tupleQueryResult.hasNext());
				while (tupleQueryResult.hasNext()) {
					BindingSet b = tupleQueryResult.next();
					assertEquals("http://p" + i, b.getBinding("p").getValue().toString());
				}
			}
		}
		logger.info("DELETE");
		sparqlQuery = new StringBuilder("DELETE DATA { ");
		for (int i = 0; i < 10; i++) {
			sparqlQuery.append("	<http://s").append(i).append(">  <http://p").append(i).append(">  <http://o")
					.append(i).append("> . ");
		}
		sparqlQuery.append("} ");
		tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();
		connection.commit();

		logger.info("QUERY");
		for (int i = 10; i < numbeOfTriples; i++) {
			sparqlQuery = new StringBuilder("SELECT ?s WHERE { ?s  <http://p" + i + ">  <http://o" + i + "> . } ");
			TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
			try (TupleQueryResult tupleQueryResult = tupleQuery1.evaluate()) {
				assertTrue(tupleQueryResult.hasNext());
				while (tupleQueryResult.hasNext()) {
					BindingSet b = tupleQueryResult.next();
					assertEquals("http://s" + i, b.getBinding("s").getValue().toString());
				}
			}
		}

		logger.info("QUERY");
		for (int i = 10; i < numbeOfTriples; i++) {
			sparqlQuery = new StringBuilder("SELECT ?p WHERE { <http://s" + i + ">  ?p  <http://o" + i + "> . } ");
			TupleQuery tupleQuery2 = connection.prepareTupleQuery(sparqlQuery.toString());
			try (TupleQueryResult tupleQueryResult2 = tupleQuery2.evaluate()) {
				assertTrue(tupleQueryResult2.hasNext());
				while (tupleQueryResult2.hasNext()) {
					BindingSet b = tupleQueryResult2.next();
					assertEquals("http://p" + i, b.getBinding("p").getValue().toString());
				}
			}
		}
		connection.close();
		endpointStore.shutDown();
	}

	@Test
	@Ignore("Can't use it anymore") // @todo: check if this true
	public void duringMerge1() throws InterruptedException {
		int threshold = 400;
		logger.info("Setting the threshold to " + threshold);
		store.setThreshold(threshold);
		SailRepository endpointStore = new SailRepository(store);

		logger.info("Insert some data");
		int numbeOfTriples = 500;
		StringBuilder sparqlQuery = new StringBuilder("INSERT DATA { ");
		for (int i = 0; i < numbeOfTriples; i++) {
			sparqlQuery.append("	<http://s").append(i).append(">  <http://p").append(i).append(">  <http://o")
					.append(i).append("> . ");
		}
		sparqlQuery.append("} ");
		RepositoryConnection connection = endpointStore.getConnection();
		Update tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();
		connection.commit();

		// START MERGE
		// artificially rise the time to merge to 5 seconds
		store.setExtendsTimeMergeBeginningAfterSwitch(2);
		sparqlQuery = new StringBuilder("INSERT DATA { <http://s130>  <http://p130>  <http://o130> . } ");
		tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();
		connection.close();
		Thread.sleep(500);
		assertTrue(store.isMerging());

		logger.info("QUERY 1");
		connection = endpointStore.getConnection();
		for (int i = 0; i < numbeOfTriples; i++) {
			sparqlQuery = new StringBuilder("SELECT ?s WHERE { ?s  <http://p" + i + ">  <http://o" + i + "> . } ");
			TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
			try (TupleQueryResult tupleQueryResult = tupleQuery1.evaluate()) {
				assertTrue(tupleQueryResult.hasNext());
				while (tupleQueryResult.hasNext()) {
					BindingSet b = tupleQueryResult.next();
					assertEquals("http://s" + i, b.getBinding("s").getValue().toString());
				}
			}
		}

		logger.info("INSERT");
		sparqlQuery = new StringBuilder("INSERT DATA { <http://s600>  <http://p600>  <http://o600> . } ");
		tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();

		sparqlQuery = new StringBuilder("INSERT DATA { <http://s600>  <http://p600>  <http://o130> . } ");
		tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();

		sparqlQuery = new StringBuilder("INSERT DATA { <http://s600>  <http://p600>  \"my_name\" . } ");
		tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();

		connection.commit();

		Thread.sleep(5000);

		logger.info("QUERY");
		sparqlQuery = new StringBuilder("SELECT ?s WHERE { ?s  <http://p600>  <http://o600> . } ");
		TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
		TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();
		assertTrue(tupleQueryResult.hasNext());
		while (tupleQueryResult.hasNext()) {
			BindingSet b = tupleQueryResult.next();
			assertEquals("http://s600", b.getBinding("s").getValue().toString());
		}

		sparqlQuery = new StringBuilder("SELECT ?s WHERE { ?s  <http://p600>  <http://o130> . } ");
		tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
		tupleQueryResult = tupleQuery1.evaluate();
		assertTrue(tupleQueryResult.hasNext());
		while (tupleQueryResult.hasNext()) {
			BindingSet b = tupleQueryResult.next();
			assertEquals("http://s600", b.getBinding("s").getValue().toString());
		}

		sparqlQuery = new StringBuilder("SELECT ?s WHERE { ?s  <http://p600>  \"my_name\" . } ");
		tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
		tupleQueryResult = tupleQuery1.evaluate();
		assertTrue(tupleQueryResult.hasNext());
		while (tupleQueryResult.hasNext()) {
			BindingSet b = tupleQueryResult.next();
			assertEquals("http://s600", b.getBinding("s").getValue().toString());
		}

		connection.close();
		logger.info("SHUTTING DOWN");
		endpointStore.shutDown();
	}

	@Test
	public void afterMerge() throws InterruptedException {
		int threshold = 100;
		logger.info("Setting the threshold to " + threshold);
		store.setThreshold(threshold);
		SailRepository endpointStore = new SailRepository(store);

		logger.info("Insert some data");
		int numbeOfTriples = 150;
		StringBuilder sparqlQuery = new StringBuilder("INSERT DATA { ");
		for (int i = 0; i < numbeOfTriples; i++) {
			sparqlQuery.append("	<http://s").append(i).append(">  <http://p").append(i).append(">  <http://o")
					.append(i).append("> . ");
		}
		sparqlQuery.append("} ");
		RepositoryConnection connection = endpointStore.getConnection();
		Update tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();
		connection.commit();

		// START MERGE
		logger.info("INSERT");
		sparqlQuery = new StringBuilder("INSERT DATA { <http://s600>  <http://p600>  <http://o600> . } ");
		tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();

		// WAIT MERGE TO FINISH
		Thread.sleep(2000);

		logger.info("QUERY 1");
		for (int i = 0; i < numbeOfTriples; i++) {
			sparqlQuery = new StringBuilder("SELECT ?s WHERE { ?s  <http://p" + i + ">  <http://o" + i + "> . } ");
			TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
			try (TupleQueryResult tupleQueryResult = tupleQuery1.evaluate()) {
				assertTrue(tupleQueryResult.hasNext());
				while (tupleQueryResult.hasNext()) {
					BindingSet b = tupleQueryResult.next();
					assertEquals("http://s" + i, b.getBinding("s").getValue().toString());
				}
			}
		}

		logger.info("QUERY");
		sparqlQuery = new StringBuilder("SELECT ?s WHERE { ?s  <http://p600>  <http://o600> . } ");
		TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
		TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();
		assertTrue(tupleQueryResult.hasNext());
		while (tupleQueryResult.hasNext()) {
			BindingSet b = tupleQueryResult.next();
			assertEquals("http://s600", b.getBinding("s").getValue().toString());
		}

		logger.info("INSERT");
		sparqlQuery = new StringBuilder("INSERT DATA { <http://s700>  <http://p700>  <http://o700> . } ");
		tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();

		logger.info("QUERY");
		sparqlQuery = new StringBuilder("SELECT ?s WHERE { ?s  <http://p700>  <http://o700> . } ");
		tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
		TupleQueryResult tupleQueryResult2 = tupleQuery1.evaluate();
		assertTrue(tupleQueryResult2.hasNext());
		while (tupleQueryResult2.hasNext()) {
			BindingSet b = tupleQueryResult2.next();
			assertEquals("http://s700", b.getBinding("s").getValue().toString());
		}

		connection.close();

		logger.info("SHUTTING DOWN");
		endpointStore.shutDown();
	}

	@Test
	public void insertExistingTriple() throws InterruptedException {
		int threshold = 100;
		logger.info("Setting the threshold to " + threshold);
		store.setThreshold(threshold);
		SailRepository endpointStore = new SailRepository(store);

		logger.info("Insert some data");
		int numbeOfTriples = 100;
		StringBuilder sparqlQuery = new StringBuilder("INSERT DATA { ");
		for (int i = 0; i < numbeOfTriples; i++) {
			sparqlQuery.append("	<http://s").append(i).append(">  <http://p").append(i).append(">  \"").append(i)
					.append("\"@pl . ");
		}
		sparqlQuery.append("} ");
		RepositoryConnection connection = endpointStore.getConnection();
		Update tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();
		connection.commit();

		// START MERGE
		logger.info("INSERT");
		sparqlQuery = new StringBuilder("INSERT DATA { <http://s100>  <http://p100>  \"100\"@pl . } ");
		tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();

		// WAIT MERGE TO FINISH
		Thread.sleep(2000);

		logger.info("INSERT");
		sparqlQuery = new StringBuilder("INSERT DATA { <http://s0>  <http://p0>  \"0\"@pl . } ");
		tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
		tupleQuery.execute();

		logger.info("QUERY");
		sparqlQuery = new StringBuilder("SELECT ?o WHERE { <http://s0>  ?p  ?o . } ");
		TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
		TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();
		assertTrue(tupleQueryResult.hasNext());
		BindingSet b = tupleQueryResult.next();
		System.out.println(b.getBinding("o").getValue().toString());
		assertEquals("\"0\"@pl", b.getBinding("o").getValue().toString());
		assertFalse(tupleQueryResult.hasNext());

		connection.close();

		logger.info("SHUTTING DOWN");
		endpointStore.shutDown();
	}

	@Test
	public void testDeleteTripleInHdtWhileMerging() throws InterruptedException {
		int threshold = 100;
		logger.info("Setting the threshold to " + threshold);
		store.setThreshold(threshold);
		store.setExtendsTimeMergeBeginningAfterSwitch(2);
		SailRepository endpointStore = new SailRepository(store);

		logger.info("Insert some data");
		int numbeOfTriples = 100;
		StringBuilder sparqlQuery = new StringBuilder("INSERT DATA { ");
		for (int i = 0; i < numbeOfTriples; i++) {
			sparqlQuery.append("	<http://s").append(i).append(">  <http://p").append(i).append(">  \"").append(i)
					.append("\"@pl . ");
		}
		sparqlQuery.append("} ");
		try (RepositoryConnection connection = endpointStore.getConnection()) {
			Update tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
			tupleQuery.execute();
			connection.commit();
			// START MERGE
			logger.info("INSERT");
			sparqlQuery = new StringBuilder("INSERT DATA { <http://s100>  <http://p100>  \"100\"@pl . } ");
			tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
			tupleQuery.execute();
			connection.commit();
		}

		logger.info("Wait for the previous merge to finish");
		Thread.sleep(3000);

		sparqlQuery = new StringBuilder("INSERT DATA { ");
		for (int i = 101; i < numbeOfTriples + 101; i++) {
			sparqlQuery.append("	<http://s").append(i).append(">  <http://p").append(i).append(">  \"").append(i)
					.append("\"@pl . ");
		}
		sparqlQuery.append("} ");
		try (RepositoryConnection connection = endpointStore.getConnection()) {
			Update tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
			tupleQuery.execute();
			connection.commit();

			// 2nd merge should happen here..
			logger.info("INSERT");
			sparqlQuery = new StringBuilder("INSERT DATA { <http://s200>  <http://p1>  \"1\"@pl . } ");
			tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
			tupleQuery.execute();
		}

		System.out.println("Triples with s200: ");
		try (RepositoryConnection connection = endpointStore.getConnection()) {
			sparqlQuery = new StringBuilder("SELECT * WHERE { <http://s200>  ?p  ?o . } ");
			TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
			tupleQuery1.evaluate().close();

			logger.info("DELETE");
			sparqlQuery = new StringBuilder("DELETE { ?s <http://p1> ?o } WHERE { ?s  <http://p1>  ?o .} ");
			Update tupleQuery = connection.prepareUpdate(sparqlQuery.toString());
			tupleQuery.execute();

			logger.info("QUERY");
			sparqlQuery = new StringBuilder("SELECT * WHERE { <http://s200>  <http://p1>  \"1\"@pl . } ");
			tupleQuery1 = connection.prepareTupleQuery(sparqlQuery.toString());
			try (TupleQueryResult tupleQueryResult = tupleQuery1.evaluate()) {
				assertFalse(tupleQueryResult.hasNext());
			}
		} finally {
			logger.info("SHUTTING DOWN");
			endpointStore.shutDown();
		}
	}
}

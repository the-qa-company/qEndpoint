package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.model.SimpleBNodeHDT;
import com.the_qa_company.qendpoint.utils.BitArrayDisk;
import org.apache.commons.io.file.PathUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EndpointStoreTest {
	public static final String HDT_INDEX_NAME = "index_tst.hdt";
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();
	HDTOptions spec;

	@Before
	public void setUp() {
		spec = HDTOptions.of(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY,
				HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH, HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
		MergeRunnableStopPoint.debug = true;
	}

	@After
	public void complete() throws InterruptedException {
		MergeRunnable.debugWaitMerge();
		MergeRunnableStopPoint.debug = false;
	}

	@Test
	public void testInstantiate() throws IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore endpoint = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME,
				spec, nativeStore.getAbsolutePath() + File.separatorChar, true);
		endpoint.init();
		endpoint.shutDown();
	}

	@Test
	public void testGetConnection() throws IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore endpoint = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME,
				spec, nativeStore.getAbsolutePath() + File.separatorChar, true);
		try {
			endpoint.getConnection().close();
		} finally {
			endpoint.shutDown();
		}
	}

	@Test
	public void testSailRepository() throws IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		SailRepository endpointStore = new SailRepository(
				new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
						nativeStore.getAbsolutePath() + File.separatorChar, true));
		endpointStore.init();
		endpointStore.shutDown();
	}

	@Test
	public void testGetSailRepositoryConnection() throws IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		SailRepository endpointStore = new SailRepository(
				new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
						nativeStore.getAbsolutePath() + File.separatorChar, true)
		// new NativeStore(nativeStore,"spoc")
		);
		try {
			try (SailRepositoryConnection connection = endpointStore.getConnection()) {
				System.out.println(connection.size());
			}
		} finally {
			endpointStore.shutDown();
		}
	}

	@Test
	public void testShutdownAndRecreate() throws IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");

		try (HDT hdt = Utility.createTempHdtIndex(tempDir, true, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore endpoint = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME,
				spec, nativeStore.getAbsolutePath() + File.separatorChar, true);

		try {
			try (NotifyingSailConnection connection = endpoint.getConnection()) {
				connection.begin();
				connection.addStatement(RDF.TYPE, RDF.TYPE, RDFS.RESOURCE);
				connection.commit();
			}
		} finally {
			endpoint.shutDown();
		}
		try {
			endpoint = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
					nativeStore.getAbsolutePath() + File.separatorChar, true);
			try (NotifyingSailConnection connection = endpoint.getConnection()) {
				connection.begin();
				connection.addStatement(RDF.TYPE, RDF.TYPE, RDFS.RESOURCE);
				connection.commit();
			}
		} finally {
			endpoint.shutDown();
		}
	}

	@Test
	public void testAddStatement() throws IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		SailRepository endpointStore = new SailRepository(
				new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
						nativeStore.getAbsolutePath() + File.separatorChar, true));

		try {
			try (RepositoryConnection connection = endpointStore.getConnection()) {
				ValueFactory vf = connection.getValueFactory();
				String ex = "http://example.com/";
				IRI ali = vf.createIRI(ex, "Ali");
				connection.add(ali, RDF.TYPE, FOAF.PERSON);
				IRI dennis = vf.createIRI(ex, "Dennis");
				connection.add(dennis, RDF.TYPE, FOAF.PERSON);
				try (RepositoryResult<Statement> stmt = connection.getStatements(null, null, null, true)) {
					int i = 0;
					while (stmt.hasNext()) {
						i++;
						stmt.next();
					}
					// one triple in hdt and 2 added to native = 3 triples
					assertEquals(3, i);
				}
			}
		} finally {
			endpointStore.shutDown();
		}
	}

	@Test
	public void testMerge() throws InterruptedException, IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + File.separatorChar, false);
		store.setThreshold(2);
		SailRepository endpointStore = new SailRepository(store);
		try {
			try (RepositoryConnection connection = endpointStore.getConnection()) {
				ValueFactory vf = connection.getValueFactory();
				String ex = "http://example.com/";
				IRI ali = vf.createIRI(ex, "Ali");
				connection.add(ali, RDF.TYPE, FOAF.PERSON);
				IRI dennis = vf.createIRI(ex, "Dennis");
				connection.add(dennis, RDF.TYPE, FOAF.PERSON);

				// with given THRESHOLD = 2, the hdt index will be merged with
				// all
				// triples from current native store
				IRI pierre = vf.createIRI(ex, "Pierre");
				connection.add(pierre, RDF.TYPE, FOAF.PERSON);

				IRI guo = vf.createIRI(ex, "Guo");
				connection.remove(guo, RDF.TYPE, FOAF.PERSON);
				// wait for merge to be done because it's on a separate thread

				RepositoryResult<Statement> sts = connection.getStatements(null, null, null, true);
				int count = 0;
				while (sts.hasNext()) {
					System.out.println(sts.next());
					count++;
				}
				// 1 triple hdt, 2 triples native a, 1 triple native b -1 triple
				// removed from hdt
				assertEquals(3, count);
				Thread.sleep(3000);

				sts = connection.getStatements(null, null, null, true);
				count = 0;
				while (sts.hasNext()) {
					System.out.println(sts.next());
					count++;
				}
				// 2 triples hdt, 0 triples native a, 1 triple native b
				assertEquals(3, count);

			}
		} finally {
			endpointStore.shutDown();
			Files.deleteIfExists(Paths.get(HDT_INDEX_NAME));
			Files.deleteIfExists(Paths.get(HDT_INDEX_NAME + ".index.v1-1"));
			Files.deleteIfExists(Paths.get("index.nt"));
		}
	}

	@Test
	public void testMergeBig() throws IOException, InterruptedException {
		MergeRunnableStopPoint.STEP2_END.debugLock();
		MergeRunnableStopPoint.STEP2_END.debugLockTest();
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");

		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, true, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		// printHDT(hdt);

		int toAdd = 15;
		int toDelete = Utility.COUNT / 100;
		try (BitArrayDisk deleted = new BitArrayDisk(Utility.COUNT)) {
			Random rnd = new Random(42);
			EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME,
					spec, nativeStore.getAbsolutePath() + File.separatorChar, false);
			store.setThreshold(2);
			SailRepository endpointStore = new SailRepository(store);
			try {
				try (RepositoryConnection connection = endpointStore.getConnection()) {
					ValueFactory vf = connection.getValueFactory();
					connection.add(Utility.getFakePersonStatement(vf, Utility.COUNT + toAdd - 2));
					connection.add(Utility.getFakePersonStatement(vf, Utility.COUNT + toAdd - 1));
					connection.add(Utility.getFakePersonStatement(vf, Utility.COUNT + toAdd));
					// should trigger merge event
				}

				MergeRunnableStopPoint.STEP2_END.debugWaitForEvent();

				try (RepositoryConnection connection = endpointStore.getConnection()) {
					ValueFactory vf = connection.getValueFactory();

					// delete toDelete persons from HDT
					connection.begin();
					for (int i = 0; i < toDelete; i++) {
						int id = rnd.nextInt(Utility.COUNT);
						connection.remove(Utility.getFakeStatement(vf, id));
						deleted.set(id, true);
					}
					connection.commit();
				}

				MergeRunnableStopPoint.STEP2_END.debugUnlockTest();

				try (RepositoryConnection connection = endpointStore.getConnection()) {
					ValueFactory vf = connection.getValueFactory();

					for (int i = 1; i <= toAdd - 3; i++) {
						connection.add(Utility.getFakePersonStatement(vf, Utility.COUNT + i));
					}

					int endCount = toAdd + Utility.COUNT - (int) deleted.countOnes();

					// wait for merge to be done because it's on a separate
					// thread

					try (RepositoryResult<Statement> sts = connection.getStatements(null, null, null, true)) {
						int count = 0;
						while (sts.hasNext()) {
							sts.next();
							count++;
						}
						// 1 triple hdt, 2 triples native a, 1 triple native b
						// -1 triple
						// removed from hdt
						assertEquals(endCount, count);
					}
					Thread.sleep(3000);

					try (RepositoryResult<Statement> sts = connection.getStatements(null, null, null, true)) {
						int count = 0;
						while (sts.hasNext()) {
							sts.next();
							count++;
						}
						// 2 triples hdt, 0 triples native a, 1 triple native b
						assertEquals(endCount, count);
					}

				}
			} finally {
				endpointStore.shutDown();
			}
		}
	}

	@Test
	public void testCommonNativeAndHdt() throws IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + File.separatorChar, false);
		store.setThreshold(10);
		SailRepository endpointStore = new SailRepository(store);
		try {

			try (RepositoryConnection connection = endpointStore.getConnection()) {
				ValueFactory vf = connection.getValueFactory();
				String ex = "http://example.com/";
				IRI ali = vf.createIRI(ex, "Ali");
				connection.add(ali, RDF.TYPE, FOAF.PERSON);
				IRI dennis = vf.createIRI(ex, "Dennis");
				connection.add(dennis, RDF.TYPE, FOAF.PERSON);

				// query everything of type PERSON
				try (RepositoryResult<Statement> query = connection.getStatements(null, RDF.TYPE, FOAF.PERSON, true)) {
					int i = 0;
					while (query.hasNext()) {
						i++;
						System.out.println(query.next());
					}
					// 1 triple in hdt and 2 added to native = 3 triples
					assertEquals(3, i);
				}
			}
		} finally {
			endpointStore.shutDown();
		}
	}

	@Test
	public void rdf4jUsedWorkflow() throws IOException {
		// not really a test, more code workflow that is used internally as one
		// store and that can be used to debug
		Path dir = tempDir.newFolder("native-store").toPath().resolve("A");
		if (Files.exists(dir)) {
			PathUtils.deleteDirectory(dir);
		}
		Files.createDirectories(dir);
		SailRepository repository = new SailRepository(new NativeStore(dir.toFile(), "spoc,posc,cosp"));
		try {
			try (SailRepositoryConnection connection = repository.getConnection()) {
				connection.begin();

				ValueFactory factory = SimpleValueFactory.getInstance();
				for (int i = 0; i < 5000; i++) {
					IRI s1 = factory.createIRI("http://s" + i);
					IRI p1 = factory.createIRI("http://p" + i);
					IRI o1 = factory.createIRI("http://o" + i);
					connection.add(factory.createStatement(s1, p1, o1));
				}
				connection.commit();
			}

			try (RepositoryConnection connection = repository.getConnection()) {
				String sparqlQuery = "SELECT ?s WHERE { ?s  <http://p1> <http://o1> . } ";
				TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
				try (TupleQueryResult tupleQueryResult = tupleQuery1.evaluate()) {
					assertTrue(tupleQueryResult.hasNext());
					// tupleQueryResult.stream().iterator().forEachRemaining(System.out::println);
				}
			}
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void testDelete() throws InterruptedException, IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + File.separatorChar, false);
		store.setThreshold(2);
		SailRepository endpointStore = new SailRepository(store);
		try {
			try (RepositoryConnection connection = endpointStore.getConnection()) {
				ValueFactory vf = new MemValueFactory();
				String ex = "http://example.com/";
				IRI ali = vf.createIRI(ex, "Ali");
				connection.begin();
				connection.add(ali, RDF.TYPE, FOAF.PERSON);
				connection.add(vf.createIRI(ex, "Dennis"), RDF.TYPE, FOAF.PERSON);
				connection.add(vf.createIRI(ex, "Pierre"), RDF.TYPE, FOAF.PERSON);
				connection.add(vf.createIRI(ex, "Clement"), RDF.TYPE, FOAF.PERSON);
				Thread.sleep(2000);
				IRI guo = vf.createIRI(ex, "Guo");
				connection.remove(guo, RDF.TYPE, FOAF.PERSON);
				connection.remove(ali, RDF.TYPE, FOAF.PERSON);
				connection.commit();
				// query everything of type PERSON
				try (RepositoryResult<Statement> it = connection.getStatements(null, null, null, true)) {
					int count = 0;
					while (it.hasNext()) {
						count++;
						it.next();
					}
					assertEquals(3, count);
				}
			}
		} finally {
			endpointStore.shutDown();
		}
	}

	@Test
	public void testIDsConversion() throws IOException, InterruptedException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + File.separatorChar, false);
		store.setThreshold(2);
		SailRepository endpointStore = new SailRepository(store);
		try {
			Set<String> subjects = new HashSet<>();
			ValueFactory vf = new MemValueFactory();
			String ex = "http://example.com/";

			subjects.add(vf.createIRI(ex, "Dennis").toString());
			subjects.add(vf.createIRI(ex, "Pierre").toString());
			subjects.add(vf.createIRI(ex, "Guo").toString());

			try (RepositoryConnection connection = endpointStore.getConnection()) {
				connection.begin();
				IRI ali = vf.createIRI(ex, "Ali");
				connection.add(ali, RDF.TYPE, FOAF.PERSON);
				connection.add(vf.createIRI(ex, "Dennis"), RDF.TYPE, FOAF.PERSON);
				connection.add(vf.createIRI(ex, "Pierre"), RDF.TYPE, FOAF.PERSON);
				IRI guo = vf.createIRI(ex, "Guo");
				connection.remove(guo, RDF.TYPE, FOAF.PERSON);

				connection.remove(ali, RDF.TYPE, FOAF.PERSON);

				connection.add(guo, RDF.TYPE, FOAF.PERSON);
				connection.commit();
				Thread.sleep(5000);
				// query everything of type PERSON
				try (RepositoryResult<Statement> query = connection.getStatements(null, null, null, true)) {
					while (query.hasNext()) {
						assertTrue(subjects.remove(query.next().getSubject().toString()));
					}
					assertTrue(subjects.isEmpty());
				}
			}
		} finally {
			endpointStore.shutDown();
		}
	}

	@Test
	public void testDeleteWhileMerge() throws IOException, InterruptedException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + File.separatorChar, false);
		store.setThreshold(2);
		SailRepository endpointStore = new SailRepository(store);
		try {
			try (RepositoryConnection connection = endpointStore.getConnection()) {
				ValueFactory vf = new MemValueFactory();
				String ex = "http://example.com/";
				IRI ali = vf.createIRI(ex, "Ali");
				connection.add(ali, RDF.TYPE, FOAF.PERSON);
				connection.add(vf.createIRI(ex, "Dennis"), RDF.TYPE, FOAF.PERSON);
				connection.add(vf.createIRI(ex, "Pierre"), RDF.TYPE, FOAF.PERSON);
				connection.add(vf.createIRI(ex, "Clement"), RDF.TYPE, FOAF.PERSON);
				IRI guo = vf.createIRI(ex, "Guo");
				connection.remove(guo, RDF.TYPE, FOAF.PERSON);
				connection.remove(ali, RDF.TYPE, FOAF.PERSON);
				// query everything of type PERSON
				try (RepositoryResult<Statement> query = connection.getStatements(null, null, null, true)) {
					int count = 0;
					while (query.hasNext()) {
						count++;
						query.next();
					}
					assertEquals(3, count);
				}
				Thread.sleep(3000);
				try (RepositoryResult<Statement> query = connection.getStatements(null, null, null, true)) {
					int count = 0;
					while (query.hasNext()) {
						count++;
						query.next();
					}
					assertEquals(3, count);
				}
			}
		} finally {
			endpointStore.shutDown();
		}
	}

	@Test
	public void sparqlTest() throws IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			try {
				hdt.search("", "", "").forEachRemaining(System.out::println);
			} catch (NotFoundException e) {
				throw new RuntimeException(e);
			}
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + File.separatorChar, false);
		store.setThreshold(10);
		SailRepository endpointStore = new SailRepository(store);

		try {
			try (RepositoryConnection connection = endpointStore.getConnection()) {
				ValueFactory vf = connection.getValueFactory();
				String ex = "http://example.com/";
				IRI ali = vf.createIRI(ex, "Ali");
				connection.add(ali, RDF.TYPE, FOAF.PERSON);
				IRI dennis = vf.createIRI(ex, "Dennis");
				connection.add(dennis, RDF.TYPE, FOAF.PERSON);

				TupleQuery tupleQuery = connection.prepareTupleQuery(
						String.join("\n", "", "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
								"PREFIX foaf: <http://xmlns.com/foaf/0.1/>", "select ?s where {",
								"	?s rdf:type foaf:Person .", "}"));

				try (TupleQueryResult result = tupleQuery.evaluate()) {
					int count = 0;
					while (result.hasNext()) {
						count++;
						System.out.println(result.next());
					}
					assertEquals(3, count);
				}
			}
		} finally {
			endpointStore.shutDown();
			Files.deleteIfExists(Paths.get(EndpointStoreTest.HDT_INDEX_NAME));
			Files.deleteIfExists(Paths.get(EndpointStoreTest.HDT_INDEX_NAME + ".index.v1-1"));
			Files.deleteIfExists(Paths.get("index.nt"));
		}
	}

	@Test
	public void sparqlDeleteTest() throws IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + File.separatorChar, false);
		store.setThreshold(2);
		SailRepository endpointStore = new SailRepository(store);

		try {
			try (RepositoryConnection connection = endpointStore.getConnection()) {
				ValueFactory vf = connection.getValueFactory();
				String ex = "http://example.com/";
				IRI ali = vf.createIRI(ex, "Ali");
				connection.add(ali, RDF.TYPE, FOAF.PERSON);

				Update update = connection.prepareUpdate(
						String.join("\n", "", "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
								"PREFIX foaf: <http://xmlns.com/foaf/0.1/>", "PREFIX ex: <http://example.com/>",
								"DELETE DATA{", "	ex:Guo rdf:type foaf:Person .", "}"));

				update.execute();
				try (RepositoryResult<Statement> result = connection.getStatements(null, null, null, (Resource) null)) {
					assertTrue(result.hasNext());
					assertEquals(ali.toString(), result.next().getSubject().toString());
					assertFalse(result.hasNext());
				}
			}
		} finally {
			endpointStore.shutDown();
			Files.deleteIfExists(Paths.get(EndpointStoreTest.HDT_INDEX_NAME));
			Files.deleteIfExists(Paths.get(EndpointStoreTest.HDT_INDEX_NAME + ".index.v1-1"));
			Files.deleteIfExists(Paths.get("index.nt"));
		}
	}

	@Test
	public void sparqlDeleteAllTest() throws IOException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, true, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		}
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + File.separatorChar, false);
		store.setThreshold(2);
		SailRepository endpointStore = new SailRepository(store);
		try {
			try (RepositoryConnection connection = endpointStore.getConnection()) {
				ValueFactory vf = connection.getValueFactory();
				String ex = "http://example.com/";
				IRI ali = vf.createIRI(ex, "Ali");
				connection.add(ali, RDF.TYPE, FOAF.PERSON);

				Update update = connection
						.prepareUpdate(String.join("\n", "", "DELETE {", "	?s ?p ?o", "}", "\n", "WHERE { ?s ?p ?o}"));

				update.execute();
				try (RepositoryResult<Statement> result = connection.getStatements(null, null, null, (Resource) null)) {
					assertFalse(result.hasNext());
				}
			}
		} finally {
			endpointStore.shutDown();
		}
	}

	@Test
	public void sparqlJoinTest() throws IOException, NotFoundException {
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec);
		assert hdt != null;
		hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		printHDT(hdt);
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + File.separatorChar, false);
		store.setThreshold(2);
		SailRepository endpointStore = new SailRepository(store);

		try {
			try (RepositoryConnection connection = endpointStore.getConnection()) {
				ValueFactory vf = connection.getValueFactory();
				String ex = "http://example.com/";
				IRI ali = vf.createIRI(ex, "Ali");
				connection.add(ali, RDF.TYPE, FOAF.PERSON);
				IRI guo = vf.createIRI(ex, "Guo");
				IRI has = vf.createIRI(ex, "has");
				connection.add(guo, has, FOAF.ACCOUNT);

				TupleQuery tupleQuery = connection.prepareTupleQuery(
						String.join("\n", "", "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
								"PREFIX foaf: <http://xmlns.com/foaf/0.1/>", "PREFIX ex: <http://example.com/>",
								"select ?s where {", "	?s rdf:type foaf:Person .", "	?s ex:has foaf:account .",

								"}"));

				try (TupleQueryResult result = tupleQuery.evaluate()) {
					assertTrue(result.hasNext());
					result.next();
					assertFalse(result.hasNext());
				}
			}
		} finally {
			endpointStore.shutDown();
			Files.deleteIfExists(Paths.get(HDT_INDEX_NAME));
			Files.deleteIfExists(Paths.get(HDT_INDEX_NAME + ".index.v1-1"));
			Files.deleteIfExists(Paths.get("index.nt"));
		}
	}

	@Test
	public void testAddLargeDataset() throws IOException, NotFoundException {
		StopWatch stopWatch = StopWatch.createStarted();
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec);
		assert hdt != null;
		hdt.saveToHDT(hdtStore.getAbsolutePath() + File.separatorChar + HDT_INDEX_NAME, null);
		printHDT(hdt);
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separatorChar, HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + File.separatorChar, false);
		store.setThreshold(1000000);
		SailRepository endpointStore = new SailRepository(store);

		try {
			try (SailRepositoryConnection connection = endpointStore.getConnection()) {
				stopWatch.stop();

				stopWatch = StopWatch.createStarted();
				connection.begin();
				int count = 100000;
				for (int i = 0; i < count; i++) {
					connection.add(RDFS.RESOURCE, RDFS.LABEL, connection.getValueFactory().createLiteral(i));
				}
				connection.commit();
				stopWatch.stop();

				// Thread.sleep(2000);
				assertEquals(count + 1, connection.size());
			}
		} finally {
			endpointStore.shutDown();
			Files.deleteIfExists(Paths.get(EndpointStoreTest.HDT_INDEX_NAME));
			Files.deleteIfExists(Paths.get(EndpointStoreTest.HDT_INDEX_NAME + ".index.v1-1"));
			Files.deleteIfExists(Paths.get("index.nt"));
		}

	}

	@Test
	public void bnodeTest() throws ParserException, IOException {
		Path dir = tempDir.newFolder().toPath();
		Path nativeStore = dir.resolve("native");
		Path hdtStore = dir.resolve("hdt");
		Files.createDirectories(hdtStore);
		try (HDT hdt = HDTManager.generateHDT(
				List.of(new TripleString("_:aaaa", "http://pppp", "\"aaaa\"^^<http://type>")).iterator(),
				Utility.EXAMPLE_NAMESPACE, spec, null)) {
			hdt.saveToHDT(hdtStore.resolve("test.hdt").toAbsolutePath().toString(), null);
		}

		EndpointStore store = new EndpointStore(new EndpointFiles(nativeStore, hdtStore, "test.hdt"), spec, false,
				true);
		SailRepository repo = new SailRepository(store);
		try {
			HDTConverter converter = store.getHdtConverter();
			Resource bnode = converter.idToSubjectHDTResource(1L);
			Assert.assertTrue(bnode instanceof BNode);
			Assert.assertTrue(bnode instanceof SimpleBNodeHDT);
			Assert.assertEquals(1L, ((SimpleBNodeHDT) bnode).getHdtId());
			Assert.assertEquals("aaaa", ((BNode) bnode).getID());
			Assert.assertEquals("_:aaaa", bnode.toString());
			try (SailRepositoryConnection connection = repo.getConnection()) {

				ValueFactory vf = connection.getValueFactory();

				System.out.println(vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test"));
				try (RepositoryResult<Statement> result = connection.getStatements(vf.createBNode("aaaa"),
						vf.createIRI("http://pppp"), vf.createLiteral("aaaa", vf.createIRI("http://type")))) {
					Assert.assertTrue(result.hasNext());
					result.next();
					Assert.assertFalse(result.hasNext());
				}
			}
		} finally {
			repo.shutDown();
		}

	}

	private void printHDT(HDT hdt) throws NotFoundException {
		IteratorTripleString it = hdt.search("", "", "");
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}
}

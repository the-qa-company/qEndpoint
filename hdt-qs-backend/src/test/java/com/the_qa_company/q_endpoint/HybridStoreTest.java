package com.the_qa_company.q_endpoint;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HybridStoreTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    HDTSpecification spec;

    @Before
    public void setUp() {
        spec = new HDTSpecification();
        spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
    }

    @Test
    public void testInstantiate() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            HybridStore hybridStore = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", true
            );
            hybridStore.shutDown();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetConneciton() {

        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            HybridStore hybridStore = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", true
            );
            try (SailConnection connection = hybridStore.getConnection()) {
            }
            hybridStore.shutDown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSailRepository() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            SailRepository hybridStore = new SailRepository(
                    new HybridStore(
                            hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", true
                    )
            );
            hybridStore.shutDown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetSailRepositoryConnection() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            SailRepository hybridStore = new SailRepository(
                    new HybridStore(
                            hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", true
                    )
//                    new NativeStore(nativeStore,"spoc")
            );
            try (SailRepositoryConnection connection = hybridStore.getConnection()) {
                System.out.println(connection.size());
            }
            hybridStore.shutDown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testShutdownAndRecreate() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");

            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, true, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            HybridStore hybridStore = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", true
            );

            try (NotifyingSailConnection connection = hybridStore.getConnection()) {
                connection.begin();
                connection.addStatement(RDF.TYPE, RDF.TYPE, RDFS.RESOURCE);
                connection.commit();
            }
            hybridStore.shutDown();
            hybridStore = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", true
            );
            try (NotifyingSailConnection connection = hybridStore.getConnection()) {
                connection.begin();
                connection.addStatement(RDF.TYPE, RDF.TYPE, RDFS.RESOURCE);
                connection.commit();
            }
            hybridStore.shutDown();
            hdt.close();
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testAddStatement() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(
                    new HybridStore(
                            hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", true
                    )
            );

            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);
                IRI dennis = vf.createIRI(ex, "Dennis");
                connection.add(dennis, RDF.TYPE, FOAF.PERSON);
                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null, true));
                // one triple in hdt and 2 added to native = 3 triples
                statements.forEach(System.out::println);
                assertEquals(3, statements.size());
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception found !");
        }
    }

    @Test
    public void testMerge() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);

            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);
                IRI dennis = vf.createIRI(ex, "Dennis");
                connection.add(dennis, RDF.TYPE, FOAF.PERSON);

                // with given THRESHOLD = 2, the hdt index will be merged with all triples from current native store
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
                // 1 triple hdt, 2 triples native a, 1 triple native b -1 triple removed from hdt
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
                Files.deleteIfExists(Paths.get("index.hdt"));
                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
                Files.deleteIfExists(Paths.get("index.nt"));

            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception found !");
        }
    }

    @Test
    @Ignore
    public void testMergeMultiple() {
        try {

            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, true, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(999);
            SailRepository hybridStore = new SailRepository(store);

            RepositoryConnection connection = hybridStore.getConnection();
            for (int i = 0; i < 5; i++) {
                System.out.println("Merging phase: " + (i + 1));
                int count = 1000;
                connection = hybridStore.getConnection();
                connection.begin();
                for (int j = i * count; j < (i + 1) * count; j++) {
                    connection.add(RDFS.RESOURCE, RDFS.LABEL, connection.getValueFactory().createLiteral(j));
                }
                connection.commit();
                System.out.println("Count before merge:" + connection.size());
                assertEquals(count * (i + 1), connection.size());
                Thread.sleep(4000);
                System.out.println("Count after merge:" + connection.size());
                assertEquals(count * (i + 1), connection.size());
            }
            assertEquals(5000, connection.size());
            Files.deleteIfExists(Paths.get("index.hdt"));
            Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
            Files.deleteIfExists(Paths.get("index.nt"));

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception found !");
        }
    }

    @Test
    public void testCommonNativeAndHdt() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(10);
            SailRepository hybridStore = new SailRepository(store);


            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);
                IRI dennis = vf.createIRI(ex, "Dennis");
                connection.add(dennis, RDF.TYPE, FOAF.PERSON);

                // query everything of type PERSON
                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, RDF.TYPE, FOAF.PERSON, true));
                for (Statement s : statements) {
                    System.out.println(s);
                }
                // 1 triple in hdt and 2 added to native = 3 triples
                assertEquals(3, statements.size());
                Files.deleteIfExists(Paths.get("index.hdt"));
                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
                Files.deleteIfExists(Paths.get("index.nt"));

            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception found !");
        }
    }

    @Test
    public void rdf4jUsedWorkflow() {
        // not really a test, more code workflow that is used internally as one store and that can be used to debug
        File dir = new File("./tests/native-store/A");
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        dir.mkdirs();
        NativeStore nativeStore = new NativeStore(dir, "spoc,posc,cosp");

        NotifyingSailConnection connection = nativeStore.getConnection();

        connection.begin();
        connection.startUpdate(null);

        ValueFactory factory = SimpleValueFactory.getInstance();
        for (int i=0; i<5000; i++) {
            IRI s1 = factory.createIRI("http://s"+i);
            IRI p1 = factory.createIRI("http://p"+i);
            IRI o1 = factory.createIRI("http://o"+i);
            connection.addStatement(s1,p1,o1);
        }
        connection.endUpdate(null);
        connection.commit();
        connection.close();

        SailRepository repository = new SailRepository(nativeStore);

        RepositoryConnection connection2 = repository.getConnection();
        String sparqlQuery = "SELECT ?s WHERE { ?s  <http://p1> <http://o1> . } ";
        TupleQuery tupleQuery1 = connection2.prepareTupleQuery(sparqlQuery);
        TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();

        assertEquals(true,tupleQueryResult.hasNext());

        if (tupleQueryResult.hasNext()){
            tupleQueryResult.stream().iterator().forEachRemaining(System.out::println);
        }
    }

    //    @Test
//    public void testMisc(){
//        try {
//            File nativeStore = tempDir.newFolder("native-store");
//            File hdtStore = tempDir.newFolder("hdt-store");
//            HDT hdt = Utility.createTempHdtIndex(tempDir, true,false);
//            assert hdt != null;
//            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
//            printHDT(hdt);
//            HybridStore store = new HybridStore(
//                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
//            );
//            //store.setThreshold(1);
//            SailRepository hybridStore = new SailRepository(store);
//
//
//            try (RepositoryConnection connection = hybridStore.getConnection()) {
//                ValueFactory vf = connection.getValueFactory();
//                String ex = "http://example.com/";
//                IRI ali = vf.createIRI(ex, "Ali");
//                connection.add(ali, RDF.TYPE, FOAF.PERSON);
//                IRI dennis = vf.createIRI(ex, "Dennis");
//                connection.add(dennis, vf.createIRI(ex,"has"), ali);
//                //Thread.sleep(2000);
//                // query everything of type PERSON
//                GraphQuery tupleQuery = connection.prepareGraphQuery(String.join("\n", "",
//                        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
//                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
//                        "PREFIX ex: <http://example.com/>",
//                        "construct where {",
//                        "	ex:Guo rdf:type ?o .",
//                        "	?s rdf:type ?o .",
//
//                        "}"));
//
//                GraphQueryResult evaluate = tupleQuery.evaluate();
//                int count = 0;
//                while (evaluate.hasNext())
//                {
//                    count++;
//                    System.out.println(evaluate.next());
//                }
//                assertEquals(2, count);
////                RepositoryResult<Statement> sts = connection.getStatements(null, null, null, true);
////                int count = 0;
////                while (sts.hasNext()){
////                    System.out.println(sts.next());
////                    count++;
////                }
////                assertEquals(3, count);
//                Files.deleteIfExists(Paths.get("index.hdt"));
//                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
//                Files.deleteIfExists(Paths.get("index.nt"));
//
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            fail("Exception found !");
//        }
//    }
    private class StatementComparator implements Comparator<Statement> {

        @Override
        public int compare(Statement o1, Statement o2) {
            if (o1.getSubject().toString().compareTo(o2.getSubject().toString()) == 0) {
                if (o1.getPredicate().toString().compareTo(o2.getPredicate().toString()) == 0) {
                    if (o1.getObject().toString().compareTo(o2.getObject().toString()) == 0) {
                        return 0;
                    } else {
                        return o1.getObject().toString().compareTo(o2.getObject().toString());
                    }
                } else {
                    return o1.getPredicate().toString().compareTo(o2.getPredicate().toString());
                }
            } else {
                return o1.getSubject().toString().compareTo(o2.getSubject().toString());
            }
        }
    }

    private void compareTriples(List<? extends Statement> stmtsAdded, List<? extends Statement> stmtsQueried) {
        stmtsQueried.sort(new StatementComparator());
        stmtsAdded.sort(new StatementComparator());

        for (int i = 0; i < stmtsAdded.size(); i++) {
            Statement stm1 = stmtsAdded.get(i);
            Statement stm2 = stmtsQueried.get(i);

            if (!(stm1.getSubject().equals(stm2.getSubject())
                    && stm1.getPredicate().equals(stm2.getPredicate())
                    && stm1.getObject().equals(stm2.getObject()))) {

                fail("Not equal: [" + stm1 + "] - [" + stm2 + "]");
            }
        }
    }

    @Test
    @Ignore
    public void testIndexGradually() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, true, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(99);
            SailRepository hybridStore = new SailRepository(store);

            try (RepositoryConnection connection = hybridStore.getConnection()) {
                try {
                    ClassLoader classLoader = getClass().getClassLoader();
                    InputStream inputStream = new FileInputStream(classLoader.getResource("cocktails.nt").getFile());
                    RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
                    rdfParser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
                    try (GraphQueryResult res = QueryResults.parseGraphBackground(inputStream, null, rdfParser)) {
                        int count = 1;
                        ArrayList<Statement> stmtsAdded = new ArrayList<>();
                        while (res.hasNext()) {
                            Statement st = res.next();
                            stmtsAdded.add(st);
                            connection.add(st);
                            if (count % 100 == 0) {
                                System.out.println("Sleeping for 2s...");
                                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null));
                                compareTriples(stmtsAdded, statements);
                            }
                            count++;
                        }
                        Thread.sleep(2000);
                    } catch (RDF4JException e) {
                        // handle unrecoverable error
                        e.printStackTrace();
                    } finally {
                        inputStream.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception found !");
        }
    }

    @Test
    public void testDelete() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);

            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = new MemValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);
                connection.add(vf.createIRI(ex, "Dennis"), RDF.TYPE, FOAF.PERSON);
                connection.add(vf.createIRI(ex, "Pierre"), RDF.TYPE, FOAF.PERSON);
                connection.add(vf.createIRI(ex, "Clement"), RDF.TYPE, FOAF.PERSON);
                Thread.sleep(2000);
                IRI guo = vf.createIRI(ex, "Guo");
                connection.remove(guo, RDF.TYPE, FOAF.PERSON);
                connection.remove(ali, RDF.TYPE, FOAF.PERSON);
                // query everything of type PERSON
                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null, true));
                for (Statement s : statements) {
                    System.out.println(s.toString());
                }
                connection.close();
                assertEquals(3, statements.size());

            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception found !");
        }
    }

    @Test
    public void testIDsConversion() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            System.out.println("HDT content");
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);
            ArrayList<IRI> subjects = new ArrayList<>();
            ValueFactory vf = new MemValueFactory();
            String ex = "http://example.com/";

            subjects.add(vf.createIRI(ex, "Dennis"));
            subjects.add(vf.createIRI(ex, "Pierre"));
            subjects.add(vf.createIRI(ex, "Guo"));

            try (RepositoryConnection connection = hybridStore.getConnection()) {

                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);
                connection.add(vf.createIRI(ex, "Dennis"), RDF.TYPE, FOAF.PERSON);
                connection.add(vf.createIRI(ex, "Pierre"), RDF.TYPE, FOAF.PERSON);
                IRI guo = vf.createIRI(ex, "Guo");
                connection.remove(guo, RDF.TYPE, FOAF.PERSON);

                connection.remove(ali, RDF.TYPE, FOAF.PERSON);

                connection.add(guo, RDF.TYPE, FOAF.PERSON);
                Thread.sleep(5000);
                // query everything of type PERSON
                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null, true));
                int index = 0;
                System.out.println(statements.size());
                for (Statement s : statements) {
                    System.out.println("here "+s.toString());
                    assertEquals(subjects.get(index).toString(), s.getSubject().toString());
                    index++;
                }
                connection.close();
                assertEquals(3, statements.size());

            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception found !");
        }
    }

    @Test
    public void testDeleteWhileMerge() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);

            try (RepositoryConnection connection = hybridStore.getConnection()) {
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
                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null, true));
                for (Statement s : statements) {
                    System.out.println(s.toString());
                }
                assertEquals(3, statements.size());
                Thread.sleep(3000);
                System.out.println("After merge:");
                statements = Iterations.asList(connection.getStatements(null, null, null, true));
                for (Statement s : statements) {
                    System.out.println(s.toString());
                }
                assertEquals(3, statements.size());
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception found !");
        }
    }

    @Test
    public void sparqlTest() throws IOException {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(10);
            SailRepository hybridStore = new SailRepository(store);


            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);
                IRI dennis = vf.createIRI(ex, "Dennis");
                connection.add(dennis, RDF.TYPE, FOAF.PERSON);

                TupleQuery tupleQuery = connection.prepareTupleQuery(String.join("\n", "",
                        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
                        "select ?s where {",
                        "	?s rdf:type foaf:Person .",
                        "}"));

                List<BindingSet> bindingSets = Iterations.asList(tupleQuery.evaluate());
                for (BindingSet binding : bindingSets) {
                    System.out.println(binding);
                }
                assertEquals(3, bindingSets.size());
                Files.deleteIfExists(Paths.get("index.hdt"));
                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
                Files.deleteIfExists(Paths.get("index.nt"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void sparqlDeleteTest() throws IOException {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);


            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);

                Update update = connection.prepareUpdate(String.join("\n", "",
                        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
                        "PREFIX ex: <http://example.com/>",
                        "DELETE DATA{",
                        "	ex:Guo rdf:type foaf:Person .",
                        "}"));

                update.execute();
                List<Statement> statements = Iterations.asList(connection.getStatements(null, null, null, (Resource) null));
                assertEquals(1, statements.size());
                for (Statement s : statements) {
                    System.out.println(s);
                    assertEquals(ali.toString(), s.getSubject().toString());
                }

                Files.deleteIfExists(Paths.get("index.hdt"));
                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
                Files.deleteIfExists(Paths.get("index.nt"));

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void sparqlDeleteAllTest() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, true, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);


            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);

                Update update = connection.prepareUpdate(String.join("\n", "",
                        "DELETE {",
                        "	?s ?p ?o",
                        "}", "\n", "WHERE { ?s ?p ?o}"));

                update.execute();
                List<Statement> statements = Iterations.asList(connection.getStatements(null, null, null, (Resource) null));
                assertEquals(0, statements.size());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void sparqlJoinTest() {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);


            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);
                IRI guo = vf.createIRI(ex, "Guo");
                IRI has = vf.createIRI(ex, "has");
                connection.add(guo, has, FOAF.ACCOUNT);

                TupleQuery tupleQuery = connection.prepareTupleQuery(String.join("\n", "",
                        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
                        "PREFIX ex: <http://example.com/>",
                        "select ?s where {",
                        "	?s rdf:type foaf:Person .",
                        "	?s ex:has foaf:account .",

                        "}"));

                List<BindingSet> bindingSets = Iterations.asList(tupleQuery.evaluate());
                for (BindingSet binding : bindingSets) {
                    System.out.println(binding);
                }
                assertEquals(1, bindingSets.size());
                connection.close();
                hybridStore.shutDown();
                Files.deleteIfExists(Paths.get("index.hdt"));
                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
                Files.deleteIfExists(Paths.get("index.nt"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAddLargeDataset() {
        try {
            StopWatch stopWatch = StopWatch.createStarted();
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
            );
            store.setThreshold(1000000);
            SailRepository hybridStore = new SailRepository(store);


            try (SailRepositoryConnection connection = hybridStore.getConnection()) {
                stopWatch.stop();


                stopWatch = StopWatch.createStarted();
                connection.begin();
                int count = 100000;
                for (int i = 0; i < count; i++) {
                    connection.add(RDFS.RESOURCE, RDFS.LABEL, connection.getValueFactory().createLiteral(i));
                }
                connection.commit();
                stopWatch.stop();

                //Thread.sleep(2000);
                assertEquals(count + 1, connection.size());

                Files.deleteIfExists(Paths.get("index.hdt"));
                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
                Files.deleteIfExists(Paths.get("index.nt"));

            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    private void printHDT(HDT hdt) throws NotFoundException {
        IteratorTripleString it = hdt.search("", "", "");
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }
}

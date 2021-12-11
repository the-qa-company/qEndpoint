import eu.qanswer.hybridstore.HybridStore;
import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.*;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

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
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            HybridStore hybridStore = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",true
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
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            HybridStore hybridStore = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",true
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
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            SailRepository hybridStore = new SailRepository(
                    new HybridStore(
                            hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",true
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
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            SailRepository hybridStore = new SailRepository(
                    new HybridStore(
                            hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",true
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

            HDT hdt = Utility.createTempHdtIndex(tempDir, true,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            HybridStore hybridStore = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",true
            );

            try (NotifyingSailConnection connection = hybridStore.getConnection()) {
                connection.begin();
                connection.addStatement(RDF.TYPE, RDF.TYPE, RDFS.RESOURCE);
                connection.commit();
            }
            hybridStore.shutDown();
            hybridStore = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",true
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
    public void testAddStatement(){
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(
                    new HybridStore(
                            hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",true
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
    public void testMerge(){
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
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
                while (sts.hasNext()){
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
    public void testMergeMultiple(){
        try {

            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, true,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
            );
            store.setThreshold(999);
            SailRepository hybridStore = new SailRepository(store);

            RepositoryConnection connection = hybridStore.getConnection();
            for (int i = 0; i < 5; i++) {
                System.out.println("Merging phase: "+(i+1));
                int count = 1000;
                connection = hybridStore.getConnection();
                connection.begin();
                for (int j = i*count; j < (i+1)*count; j++) {
                    connection.add(RDFS.RESOURCE, RDFS.LABEL, connection.getValueFactory().createLiteral(j));
                }
                connection.commit();
                System.out.println("Count before merge:"+connection.size());
                assertEquals(count*(i+1),connection.size());
                Thread.sleep(4000);
                System.out.println("Count after merge:"+connection.size());
                assertEquals(count*(i+1),connection.size());
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
    public void testCommonNativeAndHdt(){
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
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
                for (Statement s:statements) {
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
    private class StatementComparator implements Comparator<Statement>{

        @Override
        public int compare(Statement o1, Statement o2) {
            if(o1.getSubject().toString().compareTo(o2.getSubject().toString()) == 0){
                if(o1.getPredicate().toString().compareTo(o2.getPredicate().toString()) == 0){
                    if(o1.getObject().toString().compareTo(o2.getObject().toString()) == 0){
                        return 0;
                    }else{
                        return o1.getObject().toString().compareTo(o2.getObject().toString());
                    }
                }else{
                    return o1.getPredicate().toString().compareTo(o2.getPredicate().toString());
                }
            }else{
                return o1.getSubject().toString().compareTo(o2.getSubject().toString());
            }
        }
    }
    private void compareTriples(List<? extends Statement>  stmtsAdded, List<? extends Statement> stmtsQueried){
        stmtsQueried.sort(new StatementComparator());
        stmtsAdded.sort(new StatementComparator());

        for (int i = 0; i < stmtsAdded.size(); i++) {
            Statement stm1 = stmtsAdded.get(i);
            Statement stm2 = stmtsQueried.get(i);

            if( !(stm1.getSubject().equals(stm2.getSubject())
                    && stm1.getPredicate().equals(stm2.getPredicate())
                    && stm1.getObject().equals(stm2.getObject())) ){

                fail("Not equal: ["+stm1+"] - ["+stm2+"]");
            }
        }
    }
    @Test
    @Ignore
    public void testIndexGradually(){
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, true,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
            );
            store.setThreshold(99);
            SailRepository hybridStore = new SailRepository(store);

            try (RepositoryConnection connection = hybridStore.getConnection()) {
                try {
                    ClassLoader classLoader = getClass().getClassLoader();
                    InputStream inputStream = new FileInputStream(classLoader.getResource("cocktails.nt").getFile());
                    RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
                    rdfParser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
                    try (GraphQueryResult res = QueryResults.parseGraphBackground(inputStream, null,rdfParser)) {
                        int count = 1;
                        ArrayList<Statement> stmtsAdded = new ArrayList<>();
                        while (res.hasNext()) {
                            Statement st = res.next();
                            stmtsAdded.add(st);
                            connection.add(st);
                            if( count % 100  == 0){
                                System.out.println("Sleeping for 2s...");
                                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null));
                                compareTriples(stmtsAdded,statements);
                            }
                            count++;
                        }
                        Thread.sleep(2000);
                    }
                    catch (RDF4JException e) {
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
    public void testDelete(){
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);

            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = new MemValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex,"Ali");
                connection.add(ali,RDF.TYPE,FOAF.PERSON);
                connection.add(vf.createIRI(ex,"Dennis"),RDF.TYPE,FOAF.PERSON);
                connection.add(vf.createIRI(ex,"Pierre"),RDF.TYPE,FOAF.PERSON);
                connection.add(vf.createIRI(ex,"Clement"),RDF.TYPE,FOAF.PERSON);
                Thread.sleep(2000);
                IRI guo = vf.createIRI(ex,"Guo");
                connection.remove(guo,RDF.TYPE,FOAF.PERSON);
                connection.remove(ali,RDF.TYPE,FOAF.PERSON);
                // query everything of type PERSON
                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null, true));
                for (Statement s:statements) {
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
    public void testIDsConversion(){
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);
            ArrayList<IRI> subjects = new ArrayList<>();
            ValueFactory vf = new MemValueFactory();
            String ex = "http://example.com/";

            subjects.add(vf.createIRI(ex,"Dennis"));
            subjects.add(vf.createIRI(ex,"Pierre"));
            subjects.add(vf.createIRI(ex,"Guo"));

            try (RepositoryConnection connection = hybridStore.getConnection()) {

                IRI ali = vf.createIRI(ex,"Ali");
                connection.add(ali,RDF.TYPE,FOAF.PERSON);
                connection.add(vf.createIRI(ex,"Dennis"),RDF.TYPE,FOAF.PERSON);
                connection.add(vf.createIRI(ex,"Pierre"),RDF.TYPE,FOAF.PERSON);
                IRI guo = vf.createIRI(ex,"Guo");
                connection.remove(guo,RDF.TYPE,FOAF.PERSON);

                connection.remove(ali,RDF.TYPE,FOAF.PERSON);

                connection.add(guo,RDF.TYPE,FOAF.PERSON);
                Thread.sleep(5000);
                // query everything of type PERSON
                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null, true));
                int index = 0;
                for (Statement s:statements) {
                    System.out.println(s.getSubject().toString());
                    assertEquals(subjects.get(index).toString(),s.getSubject().toString());
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
    public void testDeleteWhileMerge(){
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);

            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = new MemValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex,"Ali");
                connection.add(ali,RDF.TYPE,FOAF.PERSON);
                connection.add(vf.createIRI(ex,"Dennis"),RDF.TYPE,FOAF.PERSON);
                connection.add(vf.createIRI(ex,"Pierre"),RDF.TYPE,FOAF.PERSON);
                connection.add(vf.createIRI(ex,"Clement"),RDF.TYPE,FOAF.PERSON);
                IRI guo = vf.createIRI(ex,"Guo");
                connection.remove(guo,RDF.TYPE,FOAF.PERSON);
                connection.remove(ali,RDF.TYPE,FOAF.PERSON);
                // query everything of type PERSON
                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null, true));
                for (Statement s:statements) {
                    System.out.println(s.toString());
                }
                assertEquals(3, statements.size());
                Thread.sleep(3000);
                System.out.println("After merge:");
                statements = Iterations.asList(connection.getStatements(null, null, null, true));
                for (Statement s:statements) {
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
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
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
                for (BindingSet binding:bindingSets){
                    System.out.println(binding);
                }
                assertEquals(3, bindingSets.size());
                Files.deleteIfExists(Paths.get("index.hdt"));
                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
                Files.deleteIfExists(Paths.get("index.nt"));
            }
        }catch (Exception e){
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void sparqlDeleteTest() throws IOException {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
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
                List<Statement> statements = Iterations.asList(connection.getStatements(null,null,null,(Resource)null));
                for (Statement s:statements){
                    System.out.println(s);
                    assertEquals(ali.toString(),s.getSubject().toString());
                }
                assertEquals(1, statements.size());
                Files.deleteIfExists(Paths.get("index.hdt"));
                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
                Files.deleteIfExists(Paths.get("index.nt"));

            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    @Test
    public void sparqlDeleteAllTest() throws IOException {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, true,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
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
                        "}","\n","WHERE { ?s ?p ?o}"));

                update.execute();
                List<Statement> statements = Iterations.asList(connection.getStatements(null,null,null,(Resource)null));
                assertEquals(0, statements.size());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    @Test
    public void sparqlJoinTest() throws IOException {
        try {
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
            );
            store.setThreshold(2);
            SailRepository hybridStore = new SailRepository(store);



            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);
                IRI guo = vf.createIRI(ex, "Guo");
                IRI has = vf.createIRI(ex,"has");
                connection.add(guo, has , FOAF.ACCOUNT);

                TupleQuery tupleQuery = connection.prepareTupleQuery(String.join("\n", "",
                        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
                        "PREFIX ex: <http://example.com/>",
                        "select ?s where {",
                        "	?s rdf:type foaf:Person .",
                        "	?s ex:has foaf:account .",

                        "}"));

                List<BindingSet> bindingSets = Iterations.asList(tupleQuery.evaluate());
                for (BindingSet binding:bindingSets){
                    System.out.println(binding);
                }
                assertEquals(1, bindingSets.size());
                connection.close();
                hybridStore.shutDown();
                Files.deleteIfExists(Paths.get("index.hdt"));
                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
                Files.deleteIfExists(Paths.get("index.nt"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    @Test
    public void testAddLargeDataset() {
        try {
            StopWatch stopWatch = StopWatch.createStarted();
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
            printHDT(hdt);
            HybridStore store = new HybridStore(
                    hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
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
                assertEquals(count+1, connection.size());

                Files.deleteIfExists(Paths.get("index.hdt"));
                Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
                Files.deleteIfExists(Paths.get("index.nt"));

            }
        }catch (Exception e){
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    private void printHDT(HDT hdt) throws NotFoundException {
        IteratorTripleString it = hdt.search("","","");
        while (it.hasNext()){
            System.out.println(it.next());
        }
    }

    @Test
    public void testCoherence() throws IOException, NotFoundException, InterruptedException {
        // initialize the store
        File nativeStore = new File("./tests/native-store/");
        nativeStore.mkdirs();
        File hdtStore = new File("./tests/hdt-store/");
        hdtStore.mkdirs();
        File tmp = new File("./tests/hdt-store/temp.nt");
        tmp.createNewFile();

        HDT hdt = Utility.createTempHdtIndex("/Users/Dennis/Downloads/test/hdt-store/temp.nt", true,false);
        assert hdt != null;
        hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);
        printHDT(hdt);
        HybridStore store = new HybridStore(
                hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",false
        );
        store.setThreshold(1000);
        SailRepository hybridStore = new SailRepository(store);

        // PRE MERGE PHASE
        int numbeOfTriples = 500;
        // insert some data
        String sparqlQuery = "INSERT DATA { ";
        for (int i=0; i<numbeOfTriples; i++){
            sparqlQuery += "	<http://s"+i+">  <http://p"+i+">  <http://o"+i+"> . ";
        }
        sparqlQuery += "} ";
        RepositoryConnection connection = hybridStore.getConnection();
        Update tupleQuery = connection.prepareUpdate(sparqlQuery);
        tupleQuery.execute();
        connection.commit();

        sparqlQuery = "SELECT ?s ?p ?o WHERE { ?s  ?p  ?o. } ";
        TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
        TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();
//        tupleQueryResult.stream().iterator().forEachRemaining(System.out::println);

        System.out.println("FIRST QUERY");
        // query some data
        for (int i=0; i<numbeOfTriples; i++) {
            sparqlQuery = "SELECT ?s WHERE { ?s  <http://p"+i+">  <http://o"+i+"> . } ";
            tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
            tupleQueryResult = tupleQuery1.evaluate();
            assertTrue(tupleQueryResult.hasNext());
            while (tupleQueryResult.hasNext()) {
                BindingSet b = tupleQueryResult.next();
                assertEquals("http://s"+i, b.getBinding("s").getValue().toString());
            }
        }
        System.out.println("SECOND QUERY");
        for (int i=0; i<numbeOfTriples; i++) {
            sparqlQuery = "SELECT ?p WHERE { <http://s"+i+">  ?p  <http://o"+i+"> . } ";
            tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
            tupleQueryResult = tupleQuery1.evaluate();
            assertEquals(true, tupleQueryResult.hasNext());
            while (tupleQueryResult.hasNext()) {
                BindingSet b = tupleQueryResult.next();
                assertEquals("http://p"+i, b.getBinding("p").getValue().toString());
            }
        }

        // delete some data
        System.out.println("DELETE QUERY");
        sparqlQuery = "DELETE DATA { ";
        for (int i=0; i<10; i++){
            sparqlQuery += "	<http://s"+i+">  <http://p"+i+">  <http://o"+i+"> . ";
        }
        sparqlQuery += "} ";
        tupleQuery = connection.prepareUpdate(sparqlQuery);
        tupleQuery.execute();
        connection.commit();

        // query some data
        for (int i=10; i<numbeOfTriples; i++) {
            sparqlQuery = "SELECT ?s WHERE { ?s  <http://p"+i+">  <http://o"+i+"> . } ";
            tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
            tupleQueryResult = tupleQuery1.evaluate();
            assertEquals(true, tupleQueryResult.hasNext());
            while (tupleQueryResult.hasNext()) {
                BindingSet b = tupleQueryResult.next();
                assertEquals("http://s"+i, b.getBinding("s").getValue().toString());
            }
        }

        sparqlQuery = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ";
        tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
        tupleQueryResult = tupleQuery1.evaluate();
        while (tupleQueryResult.hasNext()) {
            BindingSet b = tupleQueryResult.next();
            System.out.println(b.toString());
        }

        for (int i=10; i<numbeOfTriples; i++) {
            sparqlQuery = "SELECT ?p WHERE { <http://s"+i+">  ?p  <http://o"+i+"> . } ";
            TupleQuery tupleQuery2 = connection.prepareTupleQuery(sparqlQuery);
            TupleQueryResult tupleQueryResult2 = tupleQuery2.evaluate();
            System.out.println("i"+i);
            assertEquals(true, tupleQueryResult2.hasNext());
            while (tupleQueryResult2.hasNext()) {
                BindingSet b = tupleQueryResult2.next();
                assertEquals("http://p"+i, b.getBinding("p").getValue().toString());
            }
        }

        // START MERGE

        // insert one more triple, this should trigger the merge
        sparqlQuery = "INSERT DATA { <http://s130>  <http://p130>  <http://o130> . } ";
        tupleQuery = connection.prepareUpdate(sparqlQuery);
        tupleQuery.execute();

        connection.close();
        hybridStore.shutDown();

        //store.makeMerge();

        // convert delta during merge

        // after merge

    }
}

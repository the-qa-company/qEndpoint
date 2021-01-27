import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf4j.HybridStore;
import org.rdfhdt.hdt.triples.IteratorTripleString;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class HybridStoreTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    @Test
    public void testInstantiate() {
        try {
            NativeStore nativeA = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");

            HybridStore hybridStore = new HybridStore(nativeA,nativeB, Utility.createTempHdtIndex(tempDir, true,false),10);
            hybridStore.shutDown();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testGetConneciton() {

        try {
            NativeStore nativeA = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            HybridStore hybridStore = new HybridStore(nativeA,nativeB, Utility.createTempHdtIndex(tempDir, true,false),10);

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
            NativeStore nativeA = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            SailRepository hybridStore = new SailRepository(new HybridStore(nativeA,nativeB, Utility.createTempHdtIndex(tempDir, true,false),10));
            hybridStore.shutDown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testGetSailRepositoryConnection() {
        try {
            NativeStore nativeA = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            SailRepository hybridStore = new SailRepository(new HybridStore(nativeA,nativeB,Utility.createTempHdtIndex(tempDir, true,false),10));
            try (SailRepositoryConnection connection = hybridStore.getConnection()) {
            }
            hybridStore.shutDown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testShutdownAndRecreate() {
        try {
            NativeStore nativeA  = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");

            HDT hdt = Utility.createTempHdtIndex(tempDir, true,false);
            HybridStore hybridStore = new HybridStore(nativeA,nativeB,deleteStore,hdt,
                    System.getProperty("user.dir")+"/",10,true);

            try (NotifyingSailConnection connection = hybridStore.getConnection()) {
                connection.begin();
                connection.addStatement(RDF.TYPE, RDF.TYPE, RDFS.RESOURCE);
                connection.commit();
            }
            hybridStore.shutDown();
            hybridStore = new HybridStore(nativeA,nativeB,deleteStore,hdt,
                    System.getProperty("user.dir")+"/",10,true);
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
            NativeStore nativeA  = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(
                    new HybridStore(nativeA,nativeB,deleteStore,hdt,
                            System.getProperty("user.dir")+"/",10,true));

            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex, "Ali");
                connection.add(ali, RDF.TYPE, FOAF.PERSON);
                IRI dennis = vf.createIRI(ex, "Dennis");
                connection.add(dennis, RDF.TYPE, FOAF.PERSON);
                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null, true));
                // one triple in hdt and 2 added to native = 3 triples
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
            NativeStore nativeA  = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");
            createHDTIndex("index",false);
            HDT hdt = HDTManager.mapHDT("index.hdt");
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(
                    new HybridStore(nativeA,nativeB,deleteStore,hdt,
                            System.getProperty("user.dir")+"/",2,true)
            );

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

                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null, true));
                RepositoryResult<Statement> sts = connection.getStatements(null, null, null, true);
                while (sts.hasNext())
                    System.out.println(sts.next());
                // 1 triple hdt, 2 triples native a, 1 triple native b -1 triple removed from hdt
                assertEquals(3, statements.size());
                Thread.sleep(2000);

                statements = Iterations.asList(connection.getStatements(null, null, null, true));

                sts = connection.getStatements(null, null, null, true);
                while (sts.hasNext())
                    System.out.println(sts.next());
                // 2 triples hdt, 0 triples native a, 1 triple native b
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
    public void testMergeMultiple(){
        try {
            NativeStore nativeA  = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");
            createHDTIndex("index",true);
            HDT hdt = HDTManager.mapHDT("index.hdt");
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(
                    new HybridStore(nativeA,nativeB,deleteStore,hdt,
                            System.getProperty("user.dir")+"/",999,true)
            );

            try (RepositoryConnection connection = hybridStore.getConnection()) {

                for (int i = 0; i < 1; i++) {
                    System.out.println("Merging phase: "+(i+1));
                    int count = 1000;
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
                //assertEquals(5000, connection.size());
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
    public void testCommonNativeAndHdt(){
        try {
            NativeStore nativeA  = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");
            createHDTIndex("index",false);
            HDT hdt = HDTManager.mapHDT("index.hdt");
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(new HybridStore(nativeA,nativeB,deleteStore,
                    hdt,System.getProperty("user.dir")+"/",10,true));

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
                    System.out.println(s.getSubject());
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
    public void testDelete(){
        try {
            NativeStore nativeA  = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false,false);
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(new HybridStore(nativeA,nativeB,deleteStore,hdt,
                    System.getProperty("user.dir")+"/",10,true));

            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                IRI ali = vf.createIRI(ex,"Ali");
                connection.add(ali,RDF.TYPE,FOAF.PERSON);

                IRI guo = vf.createIRI(ex,"Guo");
                connection.remove(guo,RDF.TYPE,FOAF.PERSON);
                connection.remove(ali,RDF.TYPE,FOAF.PERSON);
                // query everything of type PERSON
                List<? extends Statement> statements = Iterations.asList(connection.getStatements(null, null, null, true));
                for (Statement s:statements) {
                    System.out.println(s.toString());
                }
                assertEquals(0, statements.size());

            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception found !");
        }
    }
    @Test
    public void sparqlTest() throws IOException {
        try {
            NativeStore nativeA = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");
            createHDTIndex("index", false);
            HDT hdt = HDTManager.mapHDT("index.hdt");
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(new HybridStore(nativeA, nativeB,deleteStore, hdt,
                    System.getProperty("user.dir") + "/", 10,true));

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
            NativeStore nativeA = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");
            createHDTIndex("index", false);
            HDT hdt = HDTManager.mapHDT("index.hdt");
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(new HybridStore(nativeA, nativeB, deleteStore,hdt,
                    System.getProperty("user.dir") + "/", 10,true));

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
    public void sparqlJoinTest() throws IOException {
        try {
            NativeStore nativeA = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");
            createHDTIndex("index", false);
            HDT hdt = HDTManager.mapHDT("index.hdt");
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(new HybridStore(nativeA, nativeB, deleteStore,hdt,
                    System.getProperty("user.dir") + "/", 10,true));

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
            NativeStore nativeA = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");
            createHDTIndex("index", false);
            HDT hdt = HDTManager.mapHDT("index.hdt");
            System.out.println("HDT: ==================");
            printHDT(hdt);
            SailRepository hybridStore = new SailRepository(new HybridStore(nativeA, nativeB, deleteStore, hdt,
                    System.getProperty("user.dir") + "/", 1000000,true));

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
    @Test
    public void misc(){
        try {
            NativeStore nativeA = new NativeStore(tempDir.newFolder("native-a"), "spoc,posc");
            NativeStore nativeB = new NativeStore(tempDir.newFolder("native-b"), "spoc,posc");
            NativeStore deleteStore = new NativeStore(tempDir.newFolder("native-delete"), "spoc,posc");

            HDT hdt = Utility.createTempHdtIndex(tempDir, false,true);
            SailRepository hybridStore = new SailRepository(new HybridStore(nativeA, nativeB, deleteStore, hdt,
                    System.getProperty("user.dir") + "/", 1000000,true));
            try (SailRepositoryConnection connection = hybridStore.getConnection()) {
                StopWatch stopWatch = StopWatch.createStarted();
                RepositoryResult<Statement> statements = connection.getStatements(null, null, FOAF.PERSON, true);
                int count = 0;
                while (statements.hasNext()) {
                    statements.next();
                    count++;
                }
                System.out.println("the count is: "+count);
                assertEquals(Utility.count,count);
                stopWatch.stop();
                System.out.println("Time to query : "+stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
        }catch (Exception e){
            e.printStackTrace();
            fail("Catched Exception");
        }
    }



    private void printHDT(HDT hdt) throws NotFoundException {
        IteratorTripleString it = hdt.search("","","");
        while (it.hasNext()){
            System.out.println(it.next());
        }
    }

    private void createHDTIndex(String fileName,boolean empty){
        try {
            String rdfInput = fileName+".nt";
            String hdtOutput = fileName+".hdt";
            File inputFile = new File(rdfInput);
            if(!empty){
                Utility.writeTempRDF(inputFile);
            }else{
                inputFile.createNewFile();
            }
            String baseURI = inputFile.getAbsolutePath();
            RDFNotation notation = RDFNotation.guess(rdfInput);
            HDTSpecification spec = new HDTSpecification();
            HDT hdt = HDTManager.generateHDT(inputFile.getAbsolutePath(),baseURI,notation,spec,null);
            hdt.saveToHDT(hdtOutput,null);
            if(hdt != null)
                hdt.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserException e) {
            e.printStackTrace();
        }
    }
}

package com.the_qa_company.q_endpoint;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.hybridstore.MergeRunnable;
import com.the_qa_company.q_endpoint.hybridstore.MergeRunnableStopPoint;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.Assert.assertEquals;

public class MergeRestartTest {
    private static final File HALT_TEST_DIR = new File("tests", "halt_test_dir");
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    @Rule
    public TemporaryFolder tempDir2 = new TemporaryFolder();
    HDTSpecification spec;

    @Before
    public void setUp() {
        spec = new HDTSpecification();
        spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
    }

    private void mergeRestartTest1(MergeRunnableStopPoint stopPoint, File root) throws IOException, InterruptedException, NotFoundException {
        File nativeStore = new File(root, "native-store");
        nativeStore.mkdirs();
        File hdtStore = new File(root, "hdt-store");
        hdtStore.mkdirs();
        HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
        hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
        printHDT(hdt);
        MergeRunnable.setStopPoint(stopPoint);
        HybridStore store = new HybridStore(
                hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
        );
        System.out.println("--- launching merge with stopPoint=" + stopPoint.name().toLowerCase());

        store.setThreshold(2);
        SailRepository hybridStore = new SailRepository(store);

        try (RepositoryConnection connection = hybridStore.getConnection()) {
            ValueFactory vf = connection.getValueFactory();
            String ex = "http://example.com/";
            IRI ali = vf.createIRI(ex, "Ali");
            System.out.println("--- add " + ali);
            connection.add(ali, RDF.TYPE, FOAF.PERSON);
            IRI dennis = vf.createIRI(ex, "Dennis");
            System.out.println("--- add " + dennis);
            connection.add(dennis, RDF.TYPE, FOAF.PERSON);

            // with given THRESHOLD = 2, the hdt index will be merged with all triples from current native store
            IRI pierre = vf.createIRI(ex, "Pierre");
            System.out.println("--- add " + pierre);
            connection.add(pierre, RDF.TYPE, FOAF.PERSON);

            IRI guo = vf.createIRI(ex, "Guo");
            System.out.println("--- remove " + guo);
            connection.remove(guo, RDF.TYPE, FOAF.PERSON);
            // wait for merge to be done because it's on a separate thread

            // 1 triple hdt, 2 triples native a, 1 triple native b -1 triple removed from hdt
            showCountAndAssert(connection, 3);
            Thread.sleep(3000);
            showCountAndAssert(connection, 3);

            Files.deleteIfExists(Paths.get("index.hdt"));
            Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
            Files.deleteIfExists(Paths.get("index.nt"));

        }

        MergeRunnable.debugWaitMerge();
    }
    private void mergeRestartTest2(File root) throws IOException, InterruptedException, NotFoundException {
        File nativeStore = new File(root, "native-store");
        File hdtStore = new File(root, "hdt-store");
        HybridStore store2 = new HybridStore(
                hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
        );
        SailRepository hybridStore2 = new SailRepository(store2);
        // wait for the complete merge
        MergeRunnable.debugWaitMerge();

        // @todo: check if no issue with the new store

        try (RepositoryConnection connection = hybridStore2.getConnection()) {
            showCountAndAssert(connection, 3);
        }

        deleteDir(nativeStore);
        deleteDir(hdtStore);
    }

    public void mergeRestartTest(MergeRunnableStopPoint stopPoint) throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest1(stopPoint, tempDir.getRoot());
        swapDir();
        mergeRestartTest2(tempDir2.getRoot());
    }
    public void startHalt() {
        MergeRunnableStopPoint.completeFailure = true;
        deleteDir(HALT_TEST_DIR);
        Assert.assertTrue(HALT_TEST_DIR.mkdirs());
    }
    public void endHalt() {
        Assert.assertTrue(HALT_TEST_DIR.delete());
    }
    /* test with throw/wait */
    @Test
    public void mergeRestartStep1StartTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.STEP1_START);
    }
    @Test
    public void mergeRestartStep1LockTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.STEP1_LOCK);
    }
    @Test
    public void mergeRestartStep1EndTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.STEP1_END);
    }
    @Test
    public void mergeRestartStep2StartTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.STEP2_START);
    }
    @Test
    public void mergeRestartStep2EndTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.STEP2_END);
    }
    @Test
    public void mergeRestartStep3StartTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.STEP3_START);
    }
    @Test
    public void mergeRestartStep3LockTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.STEP3_LOCK);
    }
    @Test
    public void mergeRestartStep3Mid1Test() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.STEP3_FILES_MID1);
    }
    @Test
    public void mergeRestartStep3Mid2Test() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.STEP3_FILES_MID2);
    }
    @Test
    public void mergeRestartStep3EndTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.STEP3_END);
    }
    @Test
    public void mergeRestartMergeEndTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.MERGE_END);
    }
    @Test
    public void mergeRestartMergeEndAfterSleepTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest(MergeRunnableStopPoint.MERGE_END_AFTER_SLEEP);
    }
    /* test with throw/wait */
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartStep1StartTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.STEP1_START, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartStep1StartTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartStep1LockTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.STEP1_LOCK, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartStep1LockTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartStep1EndTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.STEP1_END, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartStep1EndTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartStep2StartTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.STEP2_START, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartStep2StartTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartStep2EndTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.STEP2_END, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartStep2EndTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartStep3StartTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.STEP3_START, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartStep3StartTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartStep3LockTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.STEP3_LOCK, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartStep3LockTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartStep3Mid1Test() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.STEP3_FILES_MID1, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartStep3Mid1Test() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartStep3Mid2Test() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.STEP3_FILES_MID2, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartStep3Mid2Test() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartStep3EndTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.STEP3_END, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartStep3EndTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartMergeEndTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.MERGE_END, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartMergeEndTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartMergeEndAfterSleepTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.MERGE_END_AFTER_SLEEP, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartMergeEndAfterSleepTest() throws IOException, InterruptedException, NotFoundException {
        mergeRestartTest2(HALT_TEST_DIR);
        endHalt();
    }



    private void printHDT(HDT hdt) throws NotFoundException {
        IteratorTripleString it = hdt.search("", "", "");
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }

    private void deleteDir(File f) {
        try {
            Files.walkFileTree(Paths.get(f.getAbsolutePath()), new FileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.TERMINATE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void swapDir() {
        try {
            Path to = Paths.get(tempDir2.getRoot().getAbsolutePath());
            Path root = Paths.get(tempDir.getRoot().getAbsolutePath());
            Files.walkFileTree(root, new FileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    Path newPath = to.resolve(root.relativize(dir));
                    Files.createDirectories(newPath);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Path newFile = to.resolve(root.relativize(file));
                    Files.move(file, newFile);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.TERMINATE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int count(RepositoryConnection connection) {
        RepositoryResult<Statement> sts = connection.getStatements(null, null, null, true);
        int count = 0;
        while (sts.hasNext()) {
            System.out.println(sts.next());
            count++;
        }
        return count;
    }
    private void showCountAndAssert(RepositoryConnection connection, int excepted) {
        assertEquals(count(connection), excepted);
    }
}

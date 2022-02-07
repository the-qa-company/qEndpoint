package com.the_qa_company.q_endpoint;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.hybridstore.MergeRunnable;
import com.the_qa_company.q_endpoint.hybridstore.MergeRunnableStopPoint;
import com.the_qa_company.q_endpoint.utils.BitArrayDisk;
import org.eclipse.rdf4j.common.io.NioFile;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;

public class MergeRestartTest {
    private static final Logger logger = LoggerFactory.getLogger(MergeRestartTest.class);
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

    private void writeInfoCount(File f, int count) throws IOException{
        Files.writeString(Paths.get(f.getAbsolutePath()), String.valueOf(count));
    }

    private int getInfoCount(File f) throws IOException {
        return Integer.parseInt(Files.readString(Paths.get(f.getAbsolutePath())));
    }

    private void lockIfBefore(MergeRunnableStopPoint point, MergeRunnableStopPoint current) {
        if (current.ordinal() <= point.ordinal()) {
            logger.debug("locking " + point.name().toLowerCase());
            point.debugLockTest();
            point.debugLock();
        } else {
            logger.debug("pass locking " + point.name().toLowerCase());
        }
    }
    private void lockIfAfter(MergeRunnableStopPoint point, MergeRunnableStopPoint current) {
        if (current.ordinal() >= point.ordinal()) {
            logger.debug("locking " + point.name().toLowerCase());
            point.debugLockTest();
            point.debugLock();
        } else {
            logger.debug("pass locking " + point.name().toLowerCase());
        }
    }
    private void mergeRestartTest1(MergeRunnableStopPoint stopPoint, File root) throws IOException, InterruptedException, NotFoundException {
        lockIfAfter(MergeRunnableStopPoint.STEP1_START, stopPoint);
        lockIfAfter(MergeRunnableStopPoint.STEP2_START, stopPoint);
        lockIfAfter(MergeRunnableStopPoint.STEP2_END, stopPoint);
        MergeRunnable.setStopPoint(stopPoint);

        File nativeStore = new File(root, "native-store");
        nativeStore.mkdirs();
        File hdtStore = new File(root, "hdt-store");
        hdtStore.mkdirs();
        File countFile = new File(root, "count");

        int count = 4;
        HDT hdt = createTestHDT(tempDir.newFile().getAbsolutePath(), spec, count);
        hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + HybridStoreTest.HDT_INDEX_NAME, null);
        printHDT(hdt);
        writeInfoCount(countFile, count);
        HybridStore store = new HybridStore(
                hdtStore.getAbsolutePath() + "/", HybridStoreTest.HDT_INDEX_NAME, spec, nativeStore.getAbsolutePath() + "/", false
        );
        logger.debug("--- launching merge with stopPoint=" + stopPoint.name().toLowerCase());

        store.setThreshold(3);
        SailRepository hybridStore = new SailRepository(store);

        int step = 0;
        try {
            ++step;
            executeTestAdd(countFile, hybridStore, 1, ++count);
            ++step;
            executeTestAdd(countFile, hybridStore, 2, ++count);
            ++step;
            executeTestAdd(countFile, hybridStore, 3, ++count);
            ++step;
            // trigger the merge with this call
            executeTestAdd(countFile, hybridStore, 4, ++count);
            ++step;

            MergeRunnableStopPoint.STEP1_START.debugWaitForEvent();
            if (stopPoint.ordinal() >= MergeRunnableStopPoint.STEP1_START.ordinal()) {
                // with given THRESHOLD = 2, the hdt index will be merged with all triples from current native store
                executeTestRemoveRDF(countFile, hybridStore, 1, --count);
                ++step;

                executeTestCount(countFile, hybridStore);
                ++step;

                executeTestRemoveHDT(countFile, hybridStore, 1, --count);
                ++step;

                logger.debug("STEP1_TEST_BITMAP0o: {}", store.getDeleteBitMap().printInfo());
                logger.debug("STEP1_TEST_BITMAP0n: {}",
                        new BitArrayDisk(0, hdtStore.getAbsolutePath() + "/triples-delete.arr").printInfo());
                executeTestCount(countFile, hybridStore);
                ++step;
            }
            MergeRunnableStopPoint.STEP1_START.debugUnlockTest();
            // lock step1
            MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
            if (stopPoint.ordinal() >= MergeRunnableStopPoint.STEP2_START.ordinal()) {
                executeTestRemoveRDF(countFile, hybridStore, 2, --count);
                ++step;

                executeTestCount(countFile, hybridStore);
                ++step;

                executeTestRemoveHDT(countFile, hybridStore, 2, --count);
                ++step;

                logger.debug("count of deleted in hdt step2s: " + store.getDeleteBitMap().countOnes());
                executeTestCount(countFile, hybridStore);
                ++step;
            }
            MergeRunnableStopPoint.STEP2_START.debugUnlockTest();
            // step 2 stuffs
            MergeRunnableStopPoint.STEP2_END.debugWaitForEvent();
            if (stopPoint.ordinal() >= MergeRunnableStopPoint.STEP2_END.ordinal()) {
                executeTestRemoveRDF(countFile, hybridStore, 3, --count);
                ++step;

                executeTestCount(countFile, hybridStore);
                ++step;

                executeTestRemoveHDT(countFile, hybridStore, 3, --count);
                ++step;

                logger.debug("count of deleted in hdt step2e: " + store.getDeleteBitMap().countOnes());
                executeTestCount(countFile, hybridStore);
                ++step;
            }
            MergeRunnableStopPoint.STEP2_END.debugUnlockTest();
            // step 3 lock
            MergeRunnable.debugWaitMerge();
            ++step;

            executeTestCount(countFile, hybridStore);
            ++step;

        } catch (MergeRunnableStopPoint.MergeRunnableException e) {
            e.printStackTrace();
        }
        logger.debug("End step: " + step + ", count: " + count);
    }
    private void openConnection(SailRepository hybridStore, BiConsumer<ValueFactory, RepositoryConnection> accept) throws MergeRunnableStopPoint.MergeRunnableStopException {
        try {
            try (RepositoryConnection connection = hybridStore.getConnection()) {
                ValueFactory vf = connection.getValueFactory();
                accept.accept(vf, connection);
            }
        } catch (RepositoryException e) {
            Throwable e2 = e;
            while (!(e2 instanceof MergeRunnableStopPoint.MergeRunnableException)) {
                if (e2.getCause() == null) {
                    throw e; // it's not ours
                }
                e2 = e2.getCause();
            }
            throw (MergeRunnableStopPoint.MergeRunnableException) e2;
        }
    }
    private void mergeRestartTest2(MergeRunnableStopPoint point, File root) throws IOException, InterruptedException {
        lockIfBefore(MergeRunnableStopPoint.STEP2_START, point);
        lockIfBefore(MergeRunnableStopPoint.STEP2_END, point);

        File nativeStore = new File(root, "native-store");
        File hdtStore = new File(root, "hdt-store");
        File countFile = new File(root, "count");

        logger.debug("test2 restart, count of deleted in hdt: {}", new BitArrayDisk(4, hdtStore.getAbsolutePath() + "/triples-delete.arr").countOnes());

        // lock to get the test count 1 into the 2nd step
        HybridStore store2 = new HybridStore(
                hdtStore.getAbsolutePath() + "/", HybridStoreTest.HDT_INDEX_NAME, spec, nativeStore.getAbsolutePath() + "/", false
        );
        SailRepository hybridStore2 = new SailRepository(store2);
        // wait for the complete merge
        MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
        if (point.ordinal() <= MergeRunnableStopPoint.STEP2_START.ordinal()) {
            logger.debug("test count 0");
            logger.debug("count of deleted in hdt: {}", store2.getDeleteBitMap().countOnes());
            if (store2.getTempDeleteBitMap() != null)
                logger.debug("count of tmp del in hdt: {}", store2.getTempDeleteBitMap().countOnes());
            executeTestCount(countFile, hybridStore2);
        }
        MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

        MergeRunnableStopPoint.STEP2_END.debugWaitForEvent();
        if (point.ordinal() <= MergeRunnableStopPoint.STEP2_END.ordinal()) {
            logger.debug("test count 1");
            logger.debug("count of deleted in hdt: {}", store2.getDeleteBitMap().countOnes());
            if (store2.getTempDeleteBitMap() != null)
                logger.debug("count of tmp del in hdt: {}", store2.getTempDeleteBitMap().countOnes());
            executeTestCount(countFile, hybridStore2);
        }
        MergeRunnableStopPoint.STEP2_END.debugUnlockTest();
        MergeRunnable.debugWaitMerge();
        logger.debug("test count 2");
        logger.debug("count of deleted in hdt: {}", store2.getDeleteBitMap().countOnes());
        if (store2.getTempDeleteBitMap() != null)
            logger.debug("count of tmp del in hdt: {}", store2.getTempDeleteBitMap().countOnes());
        executeTestCount(countFile, hybridStore2);

        deleteDir(nativeStore);
        deleteDir(hdtStore);
    }
    private static class FileStore {
        File root1;
        File root2;
        boolean switchValue = false;

        public FileStore(File root1, File root2) {
            this.root1 = root1;
            this.root2 = root2;
        }

        public synchronized void switchValue() {
            switchValue = !switchValue;
        }

        public synchronized File getRoot() {
            return switchValue ? root2 : root1;
        }

        public synchronized File getNativeStore() {
            return new File(getRoot(), "native-store");
        }
        public synchronized File getHdtStore() {
            return new File(getRoot(), "hdt-store");
        }
        public synchronized File getCountFile() {
            return new File(getRoot(), "count");
        }
    }
    public void mergeRestartTest(MergeRunnableStopPoint stopPoint) throws IOException, InterruptedException, NotFoundException {
        FileStore store = new FileStore(tempDir.getRoot(), tempDir2.getRoot());
        Thread knowledgeThread = new Thread(() -> {
            MergeRunnableStopPoint.STEP1_TEST_BITMAP1.debugLock();
            MergeRunnableStopPoint.STEP1_TEST_BITMAP1.debugLockTest();
            MergeRunnableStopPoint.STEP1_TEST_BITMAP2.debugLock();
            MergeRunnableStopPoint.STEP1_TEST_BITMAP2.debugLockTest();

            MergeRunnableStopPoint.STEP1_TEST_BITMAP1.debugWaitForEvent();
            {

                logger.debug("STEP1_TEST_BITMAP1: {}",
                        new BitArrayDisk(4, store.getHdtStore().getAbsolutePath() + "/triples-delete.arr").printInfo());
            }
            MergeRunnableStopPoint.STEP1_TEST_BITMAP1.debugUnlockTest();

            MergeRunnableStopPoint.STEP1_TEST_BITMAP2.debugWaitForEvent();
            {

                logger.debug("STEP1_TEST_BITMAP2: {}",
                        new BitArrayDisk(4, store.getHdtStore().getAbsolutePath() + "/triples-delete.arr").printInfo());
            }
            MergeRunnableStopPoint.STEP1_TEST_BITMAP2.debugUnlockTest();
        }, "KnowledgeThread");
        knowledgeThread.start();
        mergeRestartTest1(stopPoint, tempDir.getRoot());
        MergeRunnableStopPoint.disableRequest = false;
//        MergeRunnableStopPoint.unlockAllLocks();
        swapDir();
        store.switchValue();
        mergeRestartTest2(stopPoint, tempDir2.getRoot());
    }
    public void startHalt() {
        MergeRunnableStopPoint.askCompleteFailure();
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
        mergeRestartTest(MergeRunnableStopPoint.MERGE_END_OLD_SLEEP);
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
    public void halt2MergeRestartStep1StartTest() throws IOException, InterruptedException {
        mergeRestartTest2(MergeRunnableStopPoint.STEP1_START, HALT_TEST_DIR);
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
    public void halt2MergeRestartStep1EndTest() throws IOException, InterruptedException {
        mergeRestartTest2(MergeRunnableStopPoint.STEP1_END, HALT_TEST_DIR);
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
    public void halt2MergeRestartStep2StartTest() throws IOException, InterruptedException {
        mergeRestartTest2(MergeRunnableStopPoint.STEP2_START, HALT_TEST_DIR);
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
    public void halt2MergeRestartStep2EndTest() throws IOException, InterruptedException {
        mergeRestartTest2(MergeRunnableStopPoint.STEP2_END, HALT_TEST_DIR);
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
    public void halt2MergeRestartStep3StartTest() throws IOException, InterruptedException {
        mergeRestartTest2(MergeRunnableStopPoint.STEP3_START, HALT_TEST_DIR);
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
    public void halt2MergeRestartStep3Mid1Test() throws IOException, InterruptedException {
        mergeRestartTest2(MergeRunnableStopPoint.STEP3_FILES_MID1, HALT_TEST_DIR);
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
    public void halt2MergeRestartStep3Mid2Test() throws IOException, InterruptedException {
        mergeRestartTest2(MergeRunnableStopPoint.STEP3_FILES_MID2, HALT_TEST_DIR);
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
    public void halt2MergeRestartStep3EndTest() throws IOException, InterruptedException {
        mergeRestartTest2(MergeRunnableStopPoint.STEP3_END, HALT_TEST_DIR);
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
    public void halt2MergeRestartMergeEndTest() throws IOException, InterruptedException {
        mergeRestartTest2(MergeRunnableStopPoint.MERGE_END, HALT_TEST_DIR);
        endHalt();
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void haltMergeRestartMergeEndAfterSleepTest() throws IOException, InterruptedException, NotFoundException {
        startHalt();
        mergeRestartTest1(MergeRunnableStopPoint.MERGE_END_OLD_SLEEP, HALT_TEST_DIR);
    }
    @Test
    @Ignore("should be used by hand | halt test")
    public void halt2MergeRestartMergeEndAfterSleepTest() throws IOException, InterruptedException {
        mergeRestartTest2(MergeRunnableStopPoint.MERGE_END_OLD_SLEEP, HALT_TEST_DIR);
        endHalt();
    }



    private void printHDT(HDT hdt) throws NotFoundException {
        IteratorTripleString it = hdt.search("", "", "");
        logger.debug("HDT: ");
        while (it.hasNext()) {
            logger.debug("- " + it.next());
        }
    }

    private void deleteDir(File f) {
        try {
            Files.walkFileTree(Paths.get(f.getAbsolutePath()), new FileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
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
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
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
        logger.debug("-- list");
        RepositoryResult<Statement> sts = connection.getStatements(null, null, null, true);
        int count = 0;
        while (sts.hasNext()) {
            logger.debug(String.valueOf(sts.next()));
            count++;
        }
        return count;
    }

    /**
     * create a test HDT of size n
     * @param fileName the hdt file
     * @param spec the hdt spec
     * @param testElements n
     * @return the hdt
     */
    private HDT createTestHDT(String fileName, HDTSpecification spec, int testElements) {
        try {
            File inputFile = new File(fileName);
            String baseURI = inputFile.getAbsolutePath();
            // adding triples

            ValueFactory vf = new MemValueFactory();
            try (FileOutputStream out = new FileOutputStream(inputFile)){
                RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
                writer.startRDF();
                logger.debug("Initial HDT:");
                for (int id = 1; id <= testElements; id++) {
                    Statement stm = vf.createStatement(
                            vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testHDT" + id),
                            vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
                            vf.createIRI(Utility.EXAMPLE_NAMESPACE, "Bidule")
                    );
                    logger.debug("HDT statement: " + stm);
                    writer.handleStatement(stm);
                }
                writer.endRDF();
            }

            HDT hdt = HDTManager.generateHDT(inputFile.getAbsolutePath(), baseURI, RDFNotation.NTRIPLES, spec, null);
            return HDTManager.indexedHDT(hdt, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private void executeTestRemoveHDT(File out, SailRepository repo, int id, int count) throws IOException {
        openConnection(repo, (vf, connection) -> {
            Statement stm = vf.createStatement(
                    vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testHDT" + id),
                    vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
                    vf.createIRI(Utility.EXAMPLE_NAMESPACE, "Bidule")
            );
            logger.debug("Remove statement " + stm);
            connection.remove(stm);
        });
        writeInfoCount(out, count);
    }
    private void executeTestRemoveRDF(File out, SailRepository repo, int id, int count) throws IOException {
        openConnection(repo, (vf, connection) -> {
            Statement stm = vf.createStatement(
                    vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testRDF" + id),
                    vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
                    vf.createIRI(Utility.EXAMPLE_NAMESPACE, "Bidule")
            );
            logger.debug("Remove statement " + stm);
            connection.remove(stm);
        });
        writeInfoCount(out, count);
    }
    private void executeTestAdd(File out, SailRepository repo, int id, int count) throws IOException {
        openConnection(repo, (vf, connection) -> {
            Statement stm = vf.createStatement(
                    vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testRDF" + id),
                    vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
                    vf.createIRI(Utility.EXAMPLE_NAMESPACE, "Bidule")
            );
            logger.debug("Add statement " + stm);
            connection.add(stm);
        });
        writeInfoCount(out, count);
    }
    private void executeTestCount(File out, SailRepository repo) throws IOException {
        int excepted = getInfoCount(out);
        openConnection(repo, (vf, connection) -> assertEquals(excepted, count(connection)));
    }
}

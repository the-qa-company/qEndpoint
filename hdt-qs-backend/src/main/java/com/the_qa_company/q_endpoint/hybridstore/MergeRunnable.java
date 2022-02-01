package com.the_qa_company.q_endpoint.hybridstore;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;

import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

public class MergeRunnable {
    @FunctionalInterface
    private interface ExceptionRunnable {
        void run(boolean restarting) throws InterruptedException, IOException;
    }
    private Runnable stopMergeIfException(final boolean restart, ExceptionRunnable runnable) {
        return () -> {
            try {
                runnable.run(restart);
            } catch (IOException e) {
                hybridStore.setMerging(false);
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
    }
    private static void delete(String file) {
        if (!new File(file).delete()) {
            logger.warn("Can't delete the file: " + file);
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(MergeRunnable.class);

    private final String locationHdt;
    private final String rdfInput;
    private final String hdtOutput;
    private final String previousMergeFile;

    private final HybridStore hybridStore;
    // this is for testing purposes, it extends the merging process to this amount of seconds. If -1 then it is not set.
    private int extendsTimeMergeBeginning = -1;
    private int extendsTimeMergeBeginningAfterSwitch = -1;
    private int extendsTimeMergeEnd = -1;


    public MergeRunnable(String locationHdt, HybridStore hybridStore) {
        this.hybridStore = hybridStore;
        this.locationHdt = locationHdt;
        rdfInput = locationHdt + "temp.nt";
        hdtOutput = locationHdt + "temp.hdt";
        previousMergeFile = locationHdt + "previous_merge";
    }

    private Lock createSwitchLock() {
        return hybridStore.lockToPreventNewConnections.createLock("switch-lock");
    }

    private Lock createTranslateLock() {
        return hybridStore.lockToPreventNewConnections.createLock("translate-lock");
    }

    private void waitForActiveConnections() throws InterruptedException{
        hybridStore.locksHoldByConnections.waitForActiveLocks();
    }

    /**
     * @return a new thread to merge
     */
    public Thread createThread() {
        return new Thread(stopMergeIfException(false, this::runAtStep1));
    }

    /**
     * @return an optional thread to restart a previous merge (if any)
     */
    public Optional<Thread> createRestartThread() {
        // @todo: check previous step with into previousMergeFile, return a thread with the runStep
        switch (getRestartStep()) {
        case 0:
            return Optional.of(new Thread(stopMergeIfException(true, this::runAtStep1)));
        case 1:
            return Optional.of(new Thread(stopMergeIfException(true, this::runAtStep2)));
        case 2:
            return Optional.of(new Thread(stopMergeIfException(true, this::runAtStep3)));
        default:
            return Optional.empty();
        }
    }

    private void markRestartStepCompleted(int step) throws IOException {
        Files.writeString(Paths.get(previousMergeFile), String.valueOf(step));
    }

    private int getRestartStep() {
        try {
            String text = Files.readString(Paths.get(previousMergeFile));
            return Integer.parseInt(text.trim());
        } catch (IOException | NumberFormatException e) {
            return -1;
        }
    }

    private void completedMerge() throws IOException{
        Files.delete(Paths.get(previousMergeFile));
    }

    private void sleep(int seconds, String title) {
        if (seconds != -1){
            try {
                logger.debug("It is sleeping " + title + " " + seconds);
                Thread.sleep(seconds * 1000L);
                logger.debug("Finished sleeping " + title);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private synchronized void runAtStep1(boolean restarting) throws InterruptedException, IOException {
        step1(restarting);
    }

    private void step1(boolean restarting) throws InterruptedException, IOException {
        markRestartStepCompleted(0);
        logger.info("Start Step 1");
        logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Waiting for locks");
        // create a lock so that new incoming connections don't do anything
        Lock switchLock = createSwitchLock();
        // wait for all running updates to finish
        logger.info("hereeee");
        waitForActiveConnections();
        logger.info("Can continue");

        // init the temp deletes while merging... triples that are deleted while merging might be in the newly generated HDT file
        hybridStore.initTempDump(restarting);
        hybridStore.initTempDeleteArray();

        // mark in the store that the merge process started
        hybridStore.setMerging(true);

        sleep(extendsTimeMergeBeginning, "extendsTimeMergeBeginning");

        // switching the stores
        this.hybridStore.switchStore = !this.hybridStore.switchStore;

        // write the switchStore value to disk in case, something crash we can recover
        this.hybridStore.writeWhichStore();

        // reset the count of triples to 0 after switching the stores
        this.hybridStore.setTriplesCount(0);

        sleep(extendsTimeMergeBeginningAfterSwitch, "extendsTimeMergeBeginningAfterSwitch");

        // make a copy of the delete array so that the merge thread doesn't interfere with the store data access @todo: a lock is needed here
        Files.copy(Paths.get(locationHdt + "triples-delete.arr"),
                Paths.get(locationHdt + "triples-delete-cpy.arr"));
        // release the lock so that the connections can continue
        switchLock.release();
        logger.info("Switch-Lock released");
        logger.info("End Step 1");
        markRestartStepCompleted(2);
        step2(false);
    }

    private synchronized void runAtStep2(boolean restarting) throws InterruptedException, IOException {
        reloadDataFromStep1();
        step2(restarting);
    }

    private void reloadDataFromStep1() {
        // @todo: reload data from step 1
        hybridStore.initTempDump(true);
        hybridStore.initTempDeleteArray();
        hybridStore.setMerging(true);
    }

    private synchronized void step2(boolean restarting) throws InterruptedException, IOException {
        logger.info("Start Step 2");
        // diff hdt indexes...
        logger.info("HDT Diff");
        diffIndexes(locationHdt + "index.hdt", locationHdt + "triples-delete-cpy.arr");
        logger.info("Dump all triples from the native store to file");
        RepositoryConnection nativeStoreConnection = hybridStore.getConnectionToFreezedStore();
        writeTempFile(nativeStoreConnection, rdfInput);
        nativeStoreConnection.commit();
        nativeStoreConnection.close();
        logger.info("Create HDT index from dumped file");
        createHDTDump(rdfInput, hdtOutput);
        // cat the original index and the temp index
        catIndexes(locationHdt + "new_index_diff.hdt", hdtOutput, locationHdt + "new_index.hdt");
        logger.info("CAT completed!!!!! " + locationHdt);

        markRestartStepCompleted(3);

        // delete the file after the mark if the shutdown occurs during the deletes
        delete(rdfInput);
        delete(hdtOutput);
        delete(locationHdt + "triples-delete-cpy.arr");
        delete(locationHdt + "triples-delete.arr");

        logger.info("End Step 2");

        step3(false);
    }

    private synchronized void runAtStep3(boolean restarting) throws InterruptedException, IOException {
        reloadDataFromStep2();
        step3(restarting);
    }

    private void reloadDataFromStep2() {
        reloadDataFromStep1();
        // @todo: reload data from step 2
    }

    private void step3(boolean restarting) throws InterruptedException, IOException {
        logger.info("Start Step 3");
        // index the new file
        HDT newHdt = HDTManager.mapIndexedHDT(locationHdt + "new_index.hdt", hybridStore.getHDTSpec());

        // convert all triples added to the merge store to new IDs of the new generated HDT
        logger.info("ID conversion");
        // create a lock so that new incoming connections don't do anything
        Lock translateLock = createTranslateLock();
        // wait for all running updates to finish
        logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Waiting for locks");
        waitForActiveConnections();
        logger.info("Can continue");

        this.hybridStore.resetDeleteArray(newHdt);
        newHdt.close();


        // rename new hdt to old hdt name so that they are replaces
        File newHDTFile = new File(locationHdt + "new_index.hdt");
        boolean renameNew = newHDTFile.renameTo(new File(locationHdt + "index.hdt"));
        File indexFile = new File(locationHdt + "new_index.hdt.index.v1-1");
        boolean renamedIndex = indexFile.renameTo(new File(locationHdt + "index.hdt.index.v1-1"));



        HDT tempHdt = HDTManager.mapIndexedHDT(locationHdt + "index.hdt", this.hybridStore.getHDTSpec());
        convertOldToNew(this.hybridStore.getHdt(), tempHdt);
        this.hybridStore.resetHDT(tempHdt);

        // mark the triples as deleted from the temp file stored while merge
        this.hybridStore.markDeletedTempTriples();
        logger.info("Releasing lock for ID conversion ....");
        translateLock.release();
        logger.info("Translate-Lock released");
        logger.info("Lock released");

        this.hybridStore.setMerging(false);
        this.hybridStore.isMergeTriggered = false;

        sleep(extendsTimeMergeEnd, "extendsTimeMergeEnd");

        completedMerge();
        logger.info("Merge finished");
    }

    private void diffIndexes(String hdtInput1, String bitArray) {
        String hdtOutput = locationHdt + "new_index_diff.hdt";
        try {
            File hdtOutputFile = new File(hdtOutput);
            File theDir = new File(hdtOutputFile.getAbsolutePath() + "_tmp");
            theDir.mkdirs();
            String location = theDir.getAbsolutePath() + "/";
            // @todo: should we not use the already mapped HDT file instead of remapping
            HDT hdt = HDTManager.diffHDTBit(location, hdtInput1, bitArray, this.hybridStore.getHDTSpec(), null);
            hdt.saveToHDT(hdtOutput, null);

            Files.delete(Paths.get(location + "dictionary"));
            FileUtils.deleteDirectory(theDir.getAbsoluteFile());
            hdt.close();
        } catch (Exception e) {
            hybridStore.setMerging(false);
            e.printStackTrace();
        }
    }

    private void catIndexes(String hdtInput1, String hdtInput2, String hdtOutput) throws IOException {
        HDT hdt = null;
        try {
            File file = new File(hdtOutput);
            File theDir = new File(file.getAbsolutePath() + "_tmp");
            theDir.mkdirs();
            String location = theDir.getAbsolutePath() + "/";
            logger.info(location);
            logger.info(hdtInput1);
            logger.info(hdtInput2);
            // @todo: should we not use the already mapped HDT file instead of remapping
            hdt = HDTManager.catHDT(location, hdtInput1, hdtInput2, this.hybridStore.getHDTSpec(), null);

            StopWatch sw = new StopWatch();
            hdt.saveToHDT(hdtOutput, null);
            hdt.close();
            logger.info("HDT saved to file in: " + sw.stopAndShow());
            Files.delete(Paths.get(location + "dictionary"));
            Files.delete(Paths.get(location + "triples"));
            theDir.delete();
            Files.deleteIfExists(Paths.get(hdtInput1));
            Files.deleteIfExists(Paths.get(hdtInput1 + ".index.v1-1"));
        } catch (Exception e) {
            e.printStackTrace();
            hybridStore.setMerging(false);
        } finally {
            if (hdt != null)
                hdt.close();
        }
    }

    private void createHDTDump(String rdfInput, String hdtOutput) {
        String baseURI = "file://" + rdfInput;
        try {
            StopWatch sw = new StopWatch();
            HDT hdt = HDTManager.generateHDT(new File(rdfInput).getAbsolutePath(), baseURI, RDFNotation.NTRIPLES, this.hybridStore.getHDTSpec(), null);
            logger.info("File converted in: " + sw.stopAndShow());
            hdt.saveToHDT(hdtOutput, null);
            logger.info("HDT saved to file in: " + sw.stopAndShow());
        } catch (IOException | ParserException e) {
            e.printStackTrace();
            hybridStore.setMerging(false);
        }
    }

    private void writeTempFile(RepositoryConnection connection, String file) {
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
            RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
            RepositoryResult<Statement> repositoryResult =
                    connection.getStatements(null, null, null, false);
            writer.startRDF();
            logger.debug("Content dumped file");
            while (repositoryResult.hasNext()) {
                Statement stm = repositoryResult.next();

                Resource newSubjIRI = this.hybridStore.getHdtConverter().rdf4jToHdtIDsubject(stm.getSubject());
                newSubjIRI = this.hybridStore.getHdtConverter().subjectHdtResourceToResource(newSubjIRI);

                IRI newPredIRI = this.hybridStore.getHdtConverter().rdf4jToHdtIDpredicate(stm.getPredicate());
                newPredIRI = this.hybridStore.getHdtConverter().predicateHdtResourceToResource(newPredIRI);

                Value newObjIRI = this.hybridStore.getHdtConverter().rdf4jToHdtIDobject(stm.getObject());
                newObjIRI = this.hybridStore.getHdtConverter().objectHdtResourceToResource(newObjIRI);

                Statement stmConverted = this.hybridStore.getValueFactory().createStatement(
                        newSubjIRI,
                        newPredIRI,
                        newObjIRI
                );
                logger.debug("  {} {} {}",stmConverted.getSubject(),stmConverted.getPredicate(),stmConverted.getObject());
                writer.handleStatement(stmConverted);
            }
            writer.endRDF();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            hybridStore.setMerging(false);
        }
    }

    private void convertOldToNew(HDT oldHDT, HDT newHDT) {
        logger.info("Started converting IDs in the merge store");
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            RepositoryConnection connection = this.hybridStore.getConnectionToChangingStore();
            RepositoryConnection connection2 = this.hybridStore.getConnectionToFreezedStore();
            connection2.clear();
            connection2.begin();
            RepositoryResult<Statement> statements = connection.getStatements(null, null, null);
            int count = 0;
            for (Statement s : statements) {
                count++;
                // get the string
                // convert the string using the new dictionary


                // get the old IRIs with old IDs
                HDTConverter iriConverter = new HDTConverter(this.hybridStore);
                Resource oldSubject = iriConverter.rdf4jToHdtIDsubject(s.getSubject());
                IRI oldPredicate = iriConverter.rdf4jToHdtIDpredicate(s.getPredicate());
                Value oldObject = iriConverter.rdf4jToHdtIDobject(s.getObject());

                // if the old string cannot be converted than we can keep the same
                Resource newSubjIRI = oldSubject;
                long id = newHDT.getDictionary().stringToId(oldSubject.toString(), TripleComponentRole.SUBJECT);
                if (id!= -1) {
                    newSubjIRI = iriConverter.subjectIdToIRI(id);
                }

                IRI newPredIRI = oldPredicate;
                id = newHDT.getDictionary().stringToId(oldPredicate.toString(), TripleComponentRole.PREDICATE);
                if (id != -1) {
                    newPredIRI = iriConverter.predicateIdToIRI(id);
                }
                Value newObjIRI = oldObject;
                id = newHDT.getDictionary().stringToId(oldObject.toString(), TripleComponentRole.OBJECT);
                if (id != -1) {
                   newObjIRI = iriConverter.objectIdToIRI(id);
                }
                logger.debug("old:[{} {} {}]",oldSubject,oldPredicate,oldObject);
                logger.debug("new:[{} {} {}]",newSubjIRI,newPredIRI,newObjIRI);
                connection2.add(newSubjIRI, newPredIRI, newObjIRI);
//                alternative, i.e. make inplace replacements
//                connection.remove(s.getSubject(), s.getPredicate(), s.getObject());
//                connection.add(newSubjIRI, newPredIRI, newObjIRI);

                if (count %10000==0){
                    logger.debug("Converted {}",count);
                }

            }
            connection2.commit();
            connection.clear();
            connection.close();
            connection2.close();
            this.hybridStore.switchStore = !this.hybridStore.switchStore;

            stopwatch.stop(); // optional
            logger.info("Time elapsed for conversion: " + stopwatch);

            // initialize bitmaps again with the new dictionary
            Files.deleteIfExists(Paths.get(this.locationHdt + "bitX"));
            Files.deleteIfExists(Paths.get(this.locationHdt + "bitY"));
            Files.deleteIfExists(Paths.get(this.locationHdt + "bitZ"));
            stopwatch = Stopwatch.createStarted();
            logger.info("Time elapsed to initialize native store dictionary: " + stopwatch);
        } catch (Exception e) {
            logger.error("Something went wrong during conversion of IDs in merge phase: ");
            e.printStackTrace();
            hybridStore.setMerging(false);
        }
    }

    public int getExtendsTimeMergeBeginning() {
        return extendsTimeMergeBeginning;
    }

    public int getExtendsTimeMergeBeginningAfterSwitch() {
        return extendsTimeMergeBeginningAfterSwitch;
    }

    public int getExtendsTimeMergeEnd() {
        return extendsTimeMergeEnd;
    }

    public void setExtendsTimeMergeBeginning(int extendsTimeMergeBeginning) {
        this.extendsTimeMergeBeginning = extendsTimeMergeBeginning;
    }

    public void setExtendsTimeMergeBeginningAfterSwitch(int extendsTimeMergeBeginningAfterSwitch) {
        this.extendsTimeMergeBeginningAfterSwitch = extendsTimeMergeBeginningAfterSwitch;
    }

    public void setExtendsTimeMergeEnd(int extendsTimeMergeEnd) {
        this.extendsTimeMergeEnd = extendsTimeMergeEnd;
    }
}
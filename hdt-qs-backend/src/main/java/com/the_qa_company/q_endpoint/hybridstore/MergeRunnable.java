package com.the_qa_company.q_endpoint.hybridstore;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;

import com.the_qa_company.q_endpoint.model.SimpleIRIHDT;
import com.the_qa_company.q_endpoint.utils.IRIConverter;

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
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class MergeRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MergeRunnable.class);

    private final RepositoryConnection nativeStoreConnection;

    private final String hdtIndex;
    private final String locationHdt;

    private final HDT hdt;
    private final HybridStore hybridStore;
    private final HDTSpecification spec;
    private final Lock mergeLock;
    // this is for testing purposes, it extends the merging process to this amount of seconds. If -1 then it is not set.
    private int extendsTimeMergeBeginning = -1;
    private int extendsTimeMergeBeginningAfterSwitch = -1;
    private int extendsTimeMergeEnd = -1;


    public MergeRunnable(String locationHdt, HybridStore hybridStore, Lock lock) {
        this.locationHdt = locationHdt;
        this.hdtIndex = locationHdt + "index.hdt";
        this.nativeStoreConnection = hybridStore.getRepoConnection();
        this.hybridStore = hybridStore;
        this.hdt = hybridStore.getHdt();
        this.spec = hybridStore.getHDTSpec();
        this.mergeLock = lock;
    }

    public synchronized void run() {
        // mark in the store that the merge process started
        hybridStore.isMerging = true;

        // init the temp deletes while merging...
        hybridStore.initTempDeleteArray();
        hybridStore.initTempDump();

        // wait for all running updates to finish
        try {
            hybridStore.connectionsLockManager.waitForActiveLocks();
            // release the lock so that the connections can continue
            this.mergeLock.release();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // extends the time of the merge, this is for testing purposes, extendsTimeMerge should be -1 in production
        if (extendsTimeMergeBeginning!=-1){
            try {
                logger.debug("It is sleeping extendsTimeMergeBeginning "+extendsTimeMergeBeginning);
                Thread.sleep(extendsTimeMergeBeginning*1000);
                logger.debug("Fnished sleeping extendsTimeMergeBeginning");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (hybridStore.switchStore) {
            this.hybridStore.currentStore = this.hybridStore.nativeStoreA;
            this.hybridStore.switchStore = false;
        } else {
            this.hybridStore.currentStore = this.hybridStore.nativeStoreB;
            this.hybridStore.switchStore = true;
        }
        // reset the count of triples to 0 after switching the stores
        this.hybridStore.setTriplesCount(0);
        // write the switchStore value to disk in case, something crash we can recover
        this.hybridStore.writeWhichStore();


        // extends the time of the merge, this is for testing purposes, extendsTimeMerge should be -1 in production
        if (extendsTimeMergeBeginningAfterSwitch!=-1){
            try {
                logger.debug("It is sleeping extendsTimeMergeBeginningAfterSwitch "+extendsTimeMergeBeginningAfterSwitch);
                Thread.sleep(extendsTimeMergeBeginningAfterSwitch*1000);
                logger.debug("Fnished sleeping extendsTimeMergeBeginningAfterSwitch");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }



        try {
            String rdfInput = locationHdt + "temp.nt";
            String hdtOutput = locationHdt + "temp.hdt";
            // make a copy of the delete array so that the merge thread doesn't interfere with the store data access
            Files.copy(Paths.get(locationHdt + "triples-delete.arr"),
                    Paths.get(locationHdt + "triples-delete-cpy.arr"));

            // diff hdt indexes...
            logger.info("HDT Diff");
            diffIndexes(hdtIndex, locationHdt + "triples-delete-cpy.arr");
            logger.info("Dump all triples from the native store to file");
            writeTempFile(nativeStoreConnection, rdfInput);
            logger.info("Create HDT index from dumped file");
            createHDTDump(rdfInput, hdtOutput);
            // cat the original index and the temp index
            catIndexes(locationHdt + "new_index_diff.hdt", hdtOutput, locationHdt + "new_index.hdt");
            logger.info("CAT completed!!!!! " + locationHdt);
            // empty native store
            emptyNativeStore();
            File file = new File(rdfInput);
            file.delete();
            file = new File(hdtOutput);
            file.delete();
            file = new File(locationHdt + "triples-delete-cpy.arr");
            file.delete();
            file = new File(locationHdt + "triples-delete.arr");
            file.delete();

            // add a lock here
            this.hybridStore.resetDeleteArray();  // @todo: no deletes are allowed in this moment of time!
            HDT tempHdt = HDTManager.mapIndexedHDT(hdtIndex, this.spec);
            // convert all triples added to the merge store to new IDs of the new generated HDT
            logger.info("ID conversion");
            Lock lock = hybridStore.manager.createLock("IDs conversion lock");
            convertOldToNew(this.hdt, tempHdt);
            this.hybridStore.resetHDT(tempHdt);
            logger.info("Releasing lock for ID conversion ....");
            lock.release();
            logger.info("Lock released");
            // mark the triples as deleted from the temp file stored while merge
            this.hybridStore.markDeletedTempTriples();
            this.hybridStore.isMerging = false;
            this.nativeStoreConnection.close();
            // extends the time of the merge, this is for testing purposes, extendsTimeMerge should be -1 in production
            if (extendsTimeMergeEnd!=-1){
                try {
                    logger.debug("It is sleeping extendsTimeMergeEnd "+extendsTimeMergeEnd);
                    Thread.sleep(extendsTimeMergeEnd*1000);
                    logger.debug("Finshed sleeping extendsTimeMergeEnd");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            hybridStore.isMerging = false;
            e.printStackTrace();
        }
        logger.info("Merge finished");
    }


    private void diffIndexes(String hdtInput1, String bitArray) {

        String hdtOutput = locationHdt + "new_index_diff.hdt";

        try {
            File filex = new File(hdtOutput);
            File theDir = new File(filex.getAbsolutePath() + "_tmp");
            theDir.mkdirs();
            String location = theDir.getAbsolutePath() + "/";
            HDT hdt = HDTManager.diffHDTBit(location, hdtInput1, bitArray, this.spec, null);
            hdt.saveToHDT(hdtOutput, null);

            Files.delete(Paths.get(location + "dictionary"));
//            Files.delete(Paths.get(location+"triples"));
            FileUtils.deleteDirectory(theDir.getAbsoluteFile());
            hdt.close();
//            Files.deleteIfExists(Paths.get(hdtIndex));
//            Files.deleteIfExists(Paths.get(hdtIndex+".index.v1-1"));
//            File file = new File(hdtOutput);
//            boolean renamed = file.renameTo(new File(hdtIndex));
//            File indexFile = new File(hdtOutput+".index.v1-1");
//            boolean renamedIndex = indexFile.renameTo(new File(hdtIndex+".index.v1-1"));
//            if(renamed && renamedIndex) {
//                logger.info("Replaced indexes successfully after diff");
//                hdt.close();
//            }
        } catch (Exception e) {
            hybridStore.isMerging = false;
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
            hdt = HDTManager.catHDT(location, hdtInput1, hdtInput2, this.spec, null);

            StopWatch sw = new StopWatch();
            hdt.saveToHDT(hdtOutput, null);
            hdt.close();
            logger.info("HDT saved to file in: " + sw.stopAndShow());
            Files.delete(Paths.get(location + "dictionary"));
            Files.delete(Paths.get(location + "triples"));
            theDir.delete();
            Files.deleteIfExists(Paths.get(hdtInput1));
            Files.deleteIfExists(Paths.get(hdtInput1 + ".index.v1-1"));
//            boolean renamed = file.renameTo(new File(hdtIndex));
//            File indexFile = new File(hdtOutput+".index.v1-1");
//            boolean renamedIndex = indexFile.renameTo(new File(hdtIndex+".index.v1-1"));
//            if(renamed && renamedIndex) {
//                logger.info("Replaced indexes successfully after cat");
//            }

        } catch (Exception e) {
            e.printStackTrace();
            hybridStore.isMerging = false;
        } finally {
            if (hdt != null)
                hdt.close();
        }

    }

    private void createHDTDump(String rdfInput, String hdtOutput) {
        String baseURI = "file://" + rdfInput;
        try {
            StopWatch sw = new StopWatch();
            HDT hdt = HDTManager.generateHDT(new File(rdfInput).getAbsolutePath(), baseURI, RDFNotation.NTRIPLES, this.spec, null);
            logger.info("File converted in: " + sw.stopAndShow());
            hdt.saveToHDT(hdtOutput, null);
            logger.info("HDT saved to file in: " + sw.stopAndShow());
        } catch (IOException | ParserException e) {
            e.printStackTrace();
            hybridStore.isMerging = false;
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

                Resource newSubjIRI = this.hybridStore.getIriConverter().getIRIHdtSubj(stm.getSubject());
                if (newSubjIRI instanceof SimpleIRIHDT) {
                    newSubjIRI = this.hybridStore.currentStore.getValueFactory().createIRI(newSubjIRI.stringValue());
                }
                IRI newPredIRI = this.hybridStore.getIriConverter().getIRIHdtPred(stm.getPredicate());
                if (newPredIRI instanceof SimpleIRIHDT) {
                    newPredIRI = this.hybridStore.currentStore.getValueFactory().createIRI(newPredIRI.stringValue());
                }
                Value newObjIRI = this.hybridStore.getIriConverter().getIRIHdtObj(stm.getObject());
                if (newObjIRI instanceof SimpleIRIHDT) {
                    newObjIRI = this.hybridStore.currentStore.getValueFactory().createIRI(newObjIRI.stringValue());
                }
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
            hybridStore.isMerging = false;
        }
    }

    private void convertOldToNew(HDT oldHDT, HDT newHDT) {
        logger.info("Started converting IDs in the merge store");
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            RepositoryConnection connection = this.hybridStore.getRepoConnection();

            RepositoryResult<Statement> statements = connection.getStatements(null, null, null);
            for (Statement s : statements) {
                // get the old IRIs with old IDs
                IRIConverter iriConverter = new IRIConverter(oldHDT);
                Resource oldSubject = iriConverter.getIRIHdtSubj(s.getSubject());
                IRI oldPredicate = iriConverter.getIRIHdtPred(s.getPredicate());
                Value oldObject = iriConverter.getIRIHdtObj(s.getObject());

                // if the old string cannot be converted than we can keep the same
                Resource newSubjIRI = oldSubject;
                long id = newHDT.getDictionary().stringToId(oldSubject.toString(), TripleComponentRole.SUBJECT);
                if (id!= -1) {
                    if (id <= newHDT.getDictionary().getNshared()) {
                        newSubjIRI = this.hybridStore.currentStore.getValueFactory().createIRI("http://hdt.org/SO" + id);
                    } else {
                        newSubjIRI = this.hybridStore.currentStore.getValueFactory().createIRI("http://hdt.org/S" + id);
                    }
                }

                IRI newPredIRI = oldPredicate;
                id = newHDT.getDictionary().stringToId(oldPredicate.toString(), TripleComponentRole.PREDICATE);
                if (id != -1) {
                    newPredIRI = this.hybridStore.currentStore.getValueFactory().createIRI("http://hdt.org/P" + id);
                }
                Value newObjIRI = oldObject;
                id = newHDT.getDictionary().stringToId(oldObject.toString(), TripleComponentRole.OBJECT);
                if (id != -1) {
                    if (id <= newHDT.getDictionary().getNshared()) {
                        newObjIRI = this.hybridStore.currentStore.getValueFactory().createIRI("http://hdt.org/SO" + id);
                    } else {
                        newObjIRI = this.hybridStore.currentStore.getValueFactory().createIRI("http://hdt.org/O" + id);
                    }
                }
                logger.debug("old:[{} {} {}]",oldSubject,oldPredicate,oldObject);
                logger.debug("new:[{} {} {}]",newSubjIRI,newPredIRI,newObjIRI);

                connection.remove(s.getSubject(), s.getPredicate(), s.getObject());
                connection.add(newSubjIRI, newPredIRI, newObjIRI);
            }
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
            hybridStore.isMerging = false;
        }
    }

    private void emptyNativeStore() {
        nativeStoreConnection.clear();
        nativeStoreConnection.commit();
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
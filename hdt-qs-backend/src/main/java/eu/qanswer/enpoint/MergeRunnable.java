package eu.qanswer.enpoint;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.concurrent.locks.LockManager;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.AbstractValueFactoryHDT;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.eclipse.rdf4j.model.impl.SimpleLiteralHDT;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf4j.HybridStore;
import org.rdfhdt.hdt.rdf4j.utility.HDTProps;
import org.rdfhdt.hdt.rdf4j.utility.IRIConverter;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class MergeRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MergeRunnable.class);
    private RepositoryConnection nativeStoreConnection;

    private String hdtIndex;
    private String locationHdt;

    private HDT hdt;
    private HybridStore hybridStore;
    HDTSpecification spec;
    private ValueFactory tempFactory;
    private Lock mergeLock;
    public MergeRunnable(String locationHdt, HybridStore hybridStore,Lock lock) {
        this.locationHdt = locationHdt;
        this.hdtIndex = locationHdt+"index.hdt";
        this.nativeStoreConnection = hybridStore.getRepoConnection();
        this.hybridStore = hybridStore;
        this.hdt = hybridStore.getHdt();
        this.spec = new HDTSpecification();
        spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
        tempFactory = new MemValueFactory();
        this.mergeLock = lock;
    }


    public synchronized void run() {
        hybridStore.isMerging = true;

        // wait for all running updates to finish
        try {
            hybridStore.connectionsLockManager.waitForActiveLocks();
            // release the lock so that the connections can continue
            this.mergeLock.release();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(hybridStore.switchStore){
            this.hybridStore.currentStore = this.hybridStore.nativeStoreA;
            this.hybridStore.switchStore = false;
        }else{
            this.hybridStore.currentStore = this.hybridStore.nativeStoreB;
            this.hybridStore.switchStore = true;
        }
        // write the switchStore value to disk in case, something crash we can recover
        this.hybridStore.writeWhichStore();

        // init the temp deletes while merging...
        hybridStore.initTempDeleteArray();
        hybridStore.initTempDump();
        try {
            String rdfInput = locationHdt+"temp.nt";
            String hdtOutput = locationHdt+"temp.hdt";
            // make a copy of the delete array so that the nerge thread doesn't interfere with the store data access
            Files.copy(Paths.get(locationHdt+"triples-delete.arr"),
                    Paths.get(locationHdt+"triples-delete-cpy.arr"));

            // diff hdt indexes...
            diffIndexes(hdtIndex,locationHdt+"triples-delete-cpy.arr");
            // dump all triples in native store
            writeTempFile(nativeStoreConnection,rdfInput);
            // create the hdt index for the temp dump
            createHDTDump(rdfInput,hdtOutput);
            // cat the original index and the temp index
            catIndexes(locationHdt+"new_index_diff.hdt",hdtOutput);
            // empty native store
            emptyNativeStore();
            Files.deleteIfExists(Paths.get(rdfInput));
            Files.deleteIfExists(Paths.get(hdtOutput));
            Files.deleteIfExists(Paths.get(locationHdt+"triples-delete-cpy.arr"));
            Files.deleteIfExists(Paths.get(locationHdt+"triples-delete.arr"));

            //this.hybridStore.initDeleteArray();

            this.hybridStore.resetDeleteArray();
            HDT tempHdt = HDTManager.mapIndexedHDT(hdtIndex,this.spec);
            try {
                // refresh the index
                logger.info("Refreshing the new index...");
                IteratorTripleString search = tempHdt.search("", "", "");
                if(search.hasNext())
                    search.next();
                logger.info("Refreshed!!");
            } catch (NotFoundException e) {
                e.printStackTrace();
            }
            // convert all triples added to the merge store to new IDs of the new generated HDT
            convertOldToNew(this.hdt,tempHdt);
            this.hybridStore.setHdtProps(new HDTProps(tempHdt));
            this.hybridStore.setHdt(tempHdt);
            this.hybridStore.setValueFactory(new AbstractValueFactoryHDT(tempHdt));
            // mark the triples as deleted from the temp file stored while merge
            this.hybridStore.markDeletedTempTriples();
            this.hybridStore.isMerging = false;
            this.nativeStoreConnection.close();
            Thread.sleep(1000);
        } catch (IOException | InterruptedException e) {
            hybridStore.isMerging = false;
            e.printStackTrace();
        }
    }

    private void diffIndexes(String hdtInput1,String bitArray) {

        String hdtOutput = locationHdt+"new_index_diff.hdt";

        try {
            File filex = new File(hdtOutput);
            File theDir = new File(filex.getAbsolutePath() + "_tmp");
            theDir.mkdirs();
            String location = theDir.getAbsolutePath()+"/";
            HDT hdt = HDTManager.diffHDTBit(location,hdtInput1,bitArray,this.spec,null);
            hdt.saveToHDT(hdtOutput,null);

            Files.delete(Paths.get(location+"dictionary"));
//            Files.delete(Paths.get(location+"triples"));
            FileUtils.deleteDirectory(theDir.getAbsoluteFile());

            hdt = HDTManager.indexedHDT(hdt,null);
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

    private void catIndexes(String hdtInput1,String hdtInput2) throws IOException {
        String hdtOutput = locationHdt+"new_index.hdt";
        HDT hdt = null;

        try {
            File file = new File(hdtOutput);
            File theDir = new File(file.getAbsolutePath()+"_tmp");
            theDir.mkdirs();
            String location = theDir.getAbsolutePath()+"/";
            hdt = HDTManager.catHDT(location,hdtInput1, hdtInput2 , this.spec,null);

            StopWatch sw = new StopWatch();
            hdt.saveToHDT(hdtOutput, null);
            logger.info("HDT saved to file in: "+sw.stopAndShow());
            Files.delete(Paths.get(location+"dictionary"));
            Files.delete(Paths.get(location+"triples"));
            theDir.delete();

            hdt = HDTManager.indexedHDT(hdt,null);

            Files.deleteIfExists(Paths.get(hdtInput1));
            Files.deleteIfExists(Paths.get(hdtInput1+".index.v1-1"));
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
            if(hdt != null)
                hdt.close();
        }

    }
    private void createHDTDump(String rdfInput,String hdtOutput){


        String baseURI = "file://"+rdfInput;
        RDFNotation notation = RDFNotation.guess(rdfInput);

        try {
            StopWatch sw = new StopWatch();
            HDT hdt = HDTManager.generateHDT(new File(rdfInput).getAbsolutePath(), baseURI,RDFNotation.NTRIPLES , this.spec, null);
            logger.info("File converted in: "+sw.stopAndShow());
            hdt.saveToHDT(hdtOutput, null);
            logger.info("HDT saved to file in: "+sw.stopAndShow());
        } catch (IOException | ParserException e) {
            e.printStackTrace();
            hybridStore.isMerging = false;
        }
    }
    private void writeTempFile(RepositoryConnection connection,String file){
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
            RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
            RepositoryResult<Statement> repositoryResult =
                    connection.getStatements(null,null,null,false);
            writer.startRDF();
            IRIConverter iriConverter = new IRIConverter(this.hdt);
            while (repositoryResult.hasNext()) {
                Statement stm = repositoryResult.next();
                Statement stmConverted = this.hybridStore.getValueFactory().createStatement(
                  iriConverter.getIRIHdtSubj(stm.getSubject()),
                        (IRI)iriConverter.getIRIHdtPred(stm.getPredicate()),
                        iriConverter.getIRIHdtObj(stm.getObject())
                );
                writer.handleStatement(stmConverted);
            }
            writer.endRDF();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            hybridStore.isMerging = false;
        }
    }
    private void convertOldToNew(HDT oldHDT, HDT newHDT){
        Lock lock = hybridStore.manager.createLock("IDs conversion lock");

        logger.info("Started converting IDs in the merge store");
//        try {
//            Thread.sleep(20000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try (RepositoryConnection connection = this.hybridStore.getRepoConnection()) {
                RepositoryResult<Statement> statements = connection.getStatements(null, null, null);
                IRIConverter converter = new IRIConverter(oldHDT);
                IRIConverter newConverter = new IRIConverter(newHDT);
                for (Statement s : statements) {
                    // get the old IRIs with old IDs
//                logger.info(s.toString());
                    Resource oldSubject = s.getSubject();
                    IRI oldPredicate = s.getPredicate();
                    Value oldObject = s.getObject();
                    // Convert to IRI HDT objects so that toString calls idToString
                    Resource iriHdtSubj = converter.getIRIHdtSubj(oldSubject);
                    IRI iriHdtPred = (IRI)converter.getIRIHdtPred(oldPredicate);
                    Value iriHdtObj = converter.getIRIHdtObj(oldObject);

                    // convert the strings to the new IDs in the new HDT
                    long newSubjId = newHDT.getDictionary().stringToId(iriHdtSubj.toString(), TripleComponentRole.SUBJECT);
                    Resource newSubjIRI = null;
                    if (newSubjId != -1) {
                        SimpleIRIHDT simpleIRIHDTSubj = new SimpleIRIHDT(newHDT, SimpleIRIHDT.SUBJECT_POS, newSubjId);
                        newSubjIRI = newConverter.convertSubj(simpleIRIHDTSubj);
                    } else // convert the string of old ID to the original String..
                        newSubjIRI = tempFactory.createIRI(iriHdtSubj.toString());
                    long newPredId = newHDT.getDictionary().stringToId(iriHdtPred.toString(), TripleComponentRole.PREDICATE);
                    IRI newPredIRI = null;
                    if (newPredId != -1) {
                        SimpleIRIHDT simpleIRIHDTPred = new SimpleIRIHDT(newHDT, SimpleIRIHDT.PREDICATE_POS, newPredId);
                        newPredIRI = newConverter.convertPred(simpleIRIHDTPred);
                    } else { // convert the string of old ID to the original String..
                        newPredIRI = tempFactory.createIRI(iriHdtPred.toString());
                    }
                    long newObjId = newHDT.getDictionary().stringToId(iriHdtObj.toString(), TripleComponentRole.OBJECT);
                    Value newObjIRI = null;
                    if (newObjId != -1) {
                        SimpleIRIHDT simpleIRIHDTObj = new SimpleIRIHDT(newHDT, SimpleIRIHDT.OBJECT_POS, newObjId);
                        newObjIRI = newConverter.convertObj(simpleIRIHDTObj);
                    } else {
                        if (iriHdtObj instanceof Literal || iriHdtObj instanceof SimpleLiteralHDT)
                            newObjIRI = tempFactory.createLiteral(iriHdtObj.toString());
                        else
                            newObjIRI = tempFactory.createIRI(iriHdtObj.toString());
                    }
//                System.out.println("old:["+ oldSubject+" "+oldPredicate+" "+oldObject+"]");
//                System.out.println("new:["+ newSubjIRI+" "+newPredIRI+" "+newObjIRI+"]");

                    // remove the old statements and append the new converted ones.
                    connection.remove(oldSubject, oldPredicate, oldObject);
                    connection.add(newSubjIRI, newPredIRI, newObjIRI);
                }
            }
            stopwatch.stop(); // optional
            System.out.println("Time elapsed for conversion: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }catch (Exception e){
            logger.error("Something went wrong during conversion of IDs in merge phase: "+e.getMessage());
            hybridStore.isMerging = false;
        }
        lock.release();
    }
    private void emptyNativeStore(){
        nativeStoreConnection.clear();
        nativeStoreConnection.commit();
    }
}
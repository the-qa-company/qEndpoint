package com.the_qa_company.q_endpoint.hybridstore;

import com.the_qa_company.q_endpoint.model.HybridStoreValueFactory;
import com.the_qa_company.q_endpoint.model.SimpleIRIHDT;
import com.the_qa_company.q_endpoint.utils.BitArrayDisk;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.common.concurrent.locks.LockManager;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverClient;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailStore;
import org.eclipse.rdf4j.sail.helpers.AbstractNotifyingSail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class HybridStore extends AbstractNotifyingSail implements FederatedServiceResolverClient {
    private static final Logger logger = LoggerFactory.getLogger(HybridStore.class);
    // HDT file containing the data
    private HDT hdt;
    // location of the HDT file
    private String locationHdt;
    // specs of the HDT file
    private HDTSpecification spec;
    private HDTConverter hdtConverter;

    // some cached information about the HDT store
    private HDTProps hdtProps;

    // stores to store the delta
    public AbstractNotifyingSail nativeStoreA;
    public AbstractNotifyingSail nativeStoreB;

    // location of the native store
    private String locationNative;

    // bitmap to mark which triples in HDT were deleted
    private BitArrayDisk deleteBitMap;
    // bitmap used to mark deleted triples in HDT during a merge operation
    private BitArrayDisk tempdeleteBitMap;
    // setting to put the delete map only in memory, i.e don't write to disk
    private boolean inMemDeletes;

    // bitmaps used to mark if the subject, predicate, object elements in HDT are used in the rdf4j delta store
    private BitArrayDisk bitX;
    private BitArrayDisk bitY;
    private BitArrayDisk bitZ;

    // marks if store A or store B is used
    public boolean switchStore = false;
    // file storing which store is used
    File checkFile;

    // flag if the store is merging or not
    private boolean isMerging = false;

    public boolean isMergeTriggered = false;
    // this is for testing purposes, it extends the merging process to this amount of seconds. If -1 then it is not set.
    private int extendsTimeMergeBeginning = -1;
    private int extendsTimeMergeBeginningAfterSwitch = -1;
    private int extendsTimeMergeEnd = -1;

    // threshold above which the merge process is starting
    private int threshold;

    ValueFactory valueFactory;

    private RDFWriter rdfWriterTempTriples;

    // lock manager for the merge thread
    public LockManager lockToPreventNewConnections;
    // lock manager for the connections over the current repository
    public LockManager locksHoldByConnections;

    // variable counting the current number of triples in the delta
    public long triplesCount;

    private Thread mergerThread;

    public HybridStore(HDT hdt, String locationHdt, String locationNative, boolean inMemDeletes) {

//        this.nativeStoreA = new MemoryStore();
//        this.nativeStoreB = new MemoryStore();
//        this.nativeStoreA.setDefaultIsolationLevel(IsolationLevels.NONE);
//        this.nativeStoreB.setDefaultIsolationLevel(IsolationLevels.NONE);
        this.nativeStoreA = new NativeStore(new File(locationNative + "A"), "spoc,posc,cosp");
        this.nativeStoreB = new NativeStore(new File(locationNative + "B"), "spoc,posc,cosp");

        (new File(locationNative)).mkdirs();
        this.checkFile = new File(locationNative + "which_store.check");

        // init the store before creating the check store file
        this.nativeStoreA.init();
        this.nativeStoreB.init();
        checkWhichStore();
        resetHDT(hdt);
        this.valueFactory = new HybridStoreValueFactory(hdt);
        this.threshold = 100000;
        this.locationHdt = locationHdt;

        this.inMemDeletes = inMemDeletes;
        this.hdtProps = new HDTProps(this.hdt);
        this.locationNative = locationNative;
        this.lockToPreventNewConnections = new LockManager();
        this.locksHoldByConnections = new LockManager();
        initDeleteArray();
        // initialize the count of the triples
        SailConnection connection = getChangingStore().getConnection();
        this.triplesCount = connection.size();
        connection.close();
    }

    public HybridStore(String locationHdt, HDTSpecification spec, String locationNative, boolean inMemDeletes) throws IOException {
        // load HDT file
        this(HDTManager.mapIndexedHDT(locationHdt + "index.hdt", spec), locationHdt, locationNative, inMemDeletes);
        this.spec = spec;
    }


    public void initNativeStoreDictionary(HDT hdt) {
        this.bitX = new BitArrayDisk(hdt.getDictionary().getNsubjects(), this.locationHdt + "bitX");
        this.bitY = new BitArrayDisk(hdt.getDictionary().getNpredicates(), this.locationHdt + "bitY");
        this.bitZ = new BitArrayDisk(hdt.getDictionary().getNAllObjects(), this.locationHdt + "bitZ");
        // if the bitmaps have not been initialized with the native store
        if (this.bitX.countOnes() == 0 && this.bitY.countOnes() == 0 && this.bitZ.countOnes() == 0) {
            initBitmaps();
        }
    }

    public void resetHDT(HDT hdt) {
        this.setHdt(hdt);
        this.setHdtProps(new HDTProps(hdt));
        initNativeStoreDictionary(this.hdt);
        this.setValueFactory(new HybridStoreValueFactory(hdt));
        this.hdtConverter = new HDTConverter(this);
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    // init the delete array upon the first start of the store
    public void initDeleteArray() {
        if (this.inMemDeletes)
            this.deleteBitMap = new BitArrayDisk(this.hdt.getTriples().getNumberOfElements(), true);
        else {
            this.deleteBitMap = new BitArrayDisk(this.hdt.getTriples().getNumberOfElements(), this.locationHdt + "triples-delete.arr");
        }
    }

    @Override
    protected void initializeInternal() throws SailException {
//        this.repo.init();
        this.nativeStoreA.init();
        this.nativeStoreB.init();
    }

    public void checkWhichStore() {
        if (!checkFile.exists() || !checkFile.isFile()) {
            // file does not exist, so this is the first time running the program.
            try {
                Files.createFile(checkFile.toPath());
                Files.writeString(checkFile.toPath(), "false");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // This file exists, we already ran the program previously, just read the value
            try {
                String s = Files.readString(checkFile.toPath());
                if (s.equals("false")) {
                    this.switchStore = false;
                } else if (s.equals("true")) {
                    this.switchStore = true;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void writeWhichStore() {
        try {
            if (switchStore)
                Files.writeString(checkFile.toPath(), "true");
            else
                Files.writeString(checkFile.toPath(), "false");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getThreshold() {
        return threshold;
    }

    public Sail getChangingStore() {
        if (switchStore){
            logger.debug("Changing store is B");
            return nativeStoreB;
        } else {
            logger.debug("Changing store is A");
            return nativeStoreA;
        }

    }

    public Sail getFreezedStoreStore() {
        if (!switchStore){
            logger.debug("Freezed store is B");
            return nativeStoreB;
        }
        else {
            logger.debug("Freezed store is A");
            return nativeStoreA;
        }

    }

    // force access to the store via reflection, the library does not allow directly since the method is protected
    public SailStore getCurrentSaliStore() {
        Method method = null;
        try {
            method = getChangingStore().getClass().getDeclaredMethod("getSailStore");
            method.setAccessible(true);
            return (SailStore)method.invoke(getChangingStore());
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public HDT getHdt() {
        return hdt;
    }

    public void setHdt(HDT hdt) {
        this.hdt = hdt;
    }

    @Override
    protected void shutDownInternal() throws SailException {
        logger.info("Shutdown A");
        this.nativeStoreA.shutDown();
        logger.info("Shutdown B");
        this.nativeStoreB.shutDown();
        // check also that the merge thread is finished
        logger.info("Shutdown merge");
        try {
            if (mergerThread != null) {
                mergerThread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Shutdown done");
    }

    @Override
    public boolean isWritable() throws SailException {
        if (switchStore)
            return nativeStoreB.isWritable();
        else
            return nativeStoreA.isWritable();
    }

    @Override
    public ValueFactory getValueFactory() {
        return this.valueFactory;
    }

    public void setValueFactory(ValueFactory valueFactory) {
        this.valueFactory = valueFactory;
    }

    @Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        try {
            return new HybridStoreConnection(this);
        } catch (Exception var2) {
            throw new SailException(var2);
        }
    }

    @Override
    protected void connectionClosed(SailConnection connection) {
        super.connectionClosed(connection);
        connection.close();
    }

    @Override
    public void setFederatedServiceResolver(FederatedServiceResolver federatedServiceResolver) {
//        nativeStoreA.setFederatedServiceResolver(federatedServiceResolver);
//        nativeStoreB.setFederatedServiceResolver(federatedServiceResolver);
    }

    public RepositoryConnection getConnectionToChangingStore() {
        return  new SailRepository(getChangingStore()).getConnection();
    }

    public RepositoryConnection getConnectionToFreezedStore() {
        return  new SailRepository(getFreezedStoreStore()).getConnection();
    }

    public boolean isMerging() {
        return isMerging;
    }

    public void setMerging(boolean merging) {
        isMerging = merging;
    }

    public Sail getNativeStoreA() {
        return nativeStoreA;
    }

    public Sail getNativeStoreB() {
        return nativeStoreB;
    }

    public HDTProps getHdtProps() {
        return hdtProps;
    }

    public void setHdtProps(HDTProps hdtProps) {
        this.hdtProps = hdtProps;
    }

    public BitArrayDisk getDeleteBitMap() {
        return deleteBitMap;
    }

    public void setDeleteBitMap(BitArrayDisk deleteBitMap) {
        this.deleteBitMap = deleteBitMap;
    }

    public BitArrayDisk getTempDeleteBitMap() {
        return tempdeleteBitMap;
    }

    public RDFWriter getRdfWriterTempTriples() {
        return rdfWriterTempTriples;
    }

    /*
    In case of merge, we create a new array to recover all deleted triples while merging
     */
    public void initTempDeleteArray() {
        this.tempdeleteBitMap = new BitArrayDisk(this.hdt.getTriples().getNumberOfElements(),
                this.locationHdt + "triples-delete-temp.arr");
    }

    /*
        Init temp file to store triples to be deleted from native store while merging
     */
    public void initTempDump() {
        FileOutputStream out = null;
        try {
            File file = new File(locationNative + "tempTriples.nt");
            if (!file.exists())
                file.createNewFile();
            out = new FileOutputStream(file);
            this.rdfWriterTempTriples = Rio.createWriter(RDFFormat.NTRIPLES, out);
            this.rdfWriterTempTriples.startRDF();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // creates a new array that marks the deleted triples in the new HDT file
    public void resetDeleteArray() {
        // delete array created at merge time
        try {
            HDT newHdt = HDTManager.mapIndexedHDT(locationHdt + "new_index.hdt", this.spec);
            BitArrayDisk newDeleteArray = new BitArrayDisk(newHdt.getTriples().getNumberOfElements(),
                    this.locationHdt + "triples-delete-new.arr");
            // iterate over the temp array, convert the triples and mark it as deleted in the new HDT file
            for (int i = 0; i < tempdeleteBitMap.getNumbits(); i++) {
                if (tempdeleteBitMap.access(i)) { // means that a triple has been deleted during merge
                    // find the deleted triple in the old HDT index
                    TripleID tripleID = this.hdt.getTriples().find(i + 1);
                    if (tripleID.isValid()) {

                        CharSequence subject = this.hdt.getDictionary().idToString(tripleID.getSubject(), TripleComponentRole.SUBJECT);
                        CharSequence predicate = this.hdt.getDictionary().idToString(tripleID.getPredicate(), TripleComponentRole.PREDICATE);
                        CharSequence object = this.hdt.getDictionary().idToString(tripleID.getObject(), TripleComponentRole.OBJECT);
                        // search over the given triple with the ID so that we can mark the new array..
                        IteratorTripleString hit = newHdt.searchWithId(subject, predicate, object);
                        if (hit.hasNext()) {
                            TripleString next = hit.next();
                            long newIndex = next.getIndex();
                            if (newIndex > 0)
                                newDeleteArray.set(newIndex - 1, true);
                        }
                    }
                }
            }
            newDeleteArray.force(true);
            newDeleteArray.close();
            File file = new File(this.locationHdt + "triples-delete-new.arr");
            boolean renamed = file.renameTo(new File(this.locationHdt + "triples-delete.arr"));
            if (renamed) {
                this.setDeleteBitMap(new BitArrayDisk(newHdt.getTriples().getNumberOfElements(),
                        this.locationHdt + "triples-delete.arr"));
                try {
                    Files.deleteIfExists(Paths.get(this.locationHdt + "triples-delete-temp.arr"));
                    Files.deleteIfExists(Paths.get(this.locationHdt + "triples-delete-new.arr"));
                    Files.deleteIfExists(Paths.get(locationHdt + "index.hdt"));
                    Files.deleteIfExists(Paths.get(locationHdt + "index.hdt.index.v1-1"));
                    File newHDTFile = new File(locationHdt + "new_index.hdt");
                    boolean renameNew = newHDTFile.renameTo(new File(locationHdt + "index.hdt"));
                    File indexFile = new File(locationHdt + "new_index.hdt.index.v1-1");
                    boolean renamedIndex = indexFile.renameTo(new File(locationHdt + "index.hdt.index.v1-1"));
                    if (renameNew && renamedIndex) {
                        logger.info("Replaced the new index by the old index");
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException | NotFoundException e) {
            e.printStackTrace();
        }
    }

    public void markDeletedTempTriples() {
        this.rdfWriterTempTriples.endRDF();
        try {
            InputStream inputStream = new FileInputStream(locationNative + "tempTriples.nt");
            RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
            rdfParser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
            try (GraphQueryResult res = QueryResults.parseGraphBackground(inputStream, null, rdfParser)) {
                while (res.hasNext()) {
                    Statement st = res.next();
                    IteratorTripleString search = this.hdt.searchWithId(st.getSubject().toString(), st.getPredicate().toString(), st.getObject().toString());
                    if (search.hasNext()) {
                        TripleString next = search.next();
                        long index = next.getIndex();
                        if (index > 0)
                            this.deleteBitMap.set(index - 1, true);
                    }
                }
            } catch (RDF4JException | NotFoundException e) {
                // handle unrecoverable error
                e.printStackTrace();
            } finally {
                inputStream.close();
                //Files.deleteIfExists(Paths.get(locationNative+"tempTriples.nt"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // called from a locked block
    private void initBitmaps() {
        logger.debug("Resetting bitmaps");
        try {
            HDTConverter converter = new HDTConverter(this);
            // iterate over the current rdf4j store and mark in HDT the store the subject, predicate, objects that are used in rdf4j
            try (RepositoryConnection connection = this.getConnectionToChangingStore()) {
                RepositoryResult<Statement> statements = connection.getStatements(null, null, null);
                for (Statement statement : statements) {
                    Resource internalSubj = converter.rdf4jToHdtIDsubject(statement.getSubject());
                    IRI internalPredicate = converter.rdf4jToHdtIDpredicate(statement.getPredicate());
                    Value internalObject = converter.rdf4jToHdtIDobject(statement.getObject());
                    this.modifyBitmaps(internalSubj, internalPredicate, internalObject);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void modifyBitmaps(Resource subject, IRI predicate, Value object) {
        // mark in HDT the store the subject, predicate, objects that are used in rdf4j
        long subjectID = -1;
        if (subject instanceof SimpleIRIHDT) {
            subjectID = ((SimpleIRIHDT)subject).getId();
        }
        long predicateID = -1;
        if (predicate  instanceof SimpleIRIHDT) {
            predicateID = ((SimpleIRIHDT)predicate).getId();
        }
        long objectID = -1;
        if (object  instanceof SimpleIRIHDT) {
            objectID = ((SimpleIRIHDT)object).getId();
        }
        modifyBitmaps(subjectID,predicateID,objectID);
    }

    public void modifyBitmaps(long subject, long predicate, long object) {
        // mark in HDT the store the subject, predicate, objects that are used in rdf4j
        if (subject != -1 && subject != 0) {
            this.getBitX().set(subject - 1, true);
        }
        if (predicate != -1 && predicate != 0) {
            this.getBitY().set(predicate - 1, true);
        }
        if (object != -1 && object != 0) {
            if (object <= this.hdt.getDictionary().getNshared()) {
                this.getBitX().set(object - 1, true);
            } else {
                this.getBitZ().set(object - hdt.getDictionary().getNshared() - 1, true);
            }
        }
    }

    // @todo: this can be dangerous, what if it is called 2 times, then two threads will start which will overlap each other, only one should be allowed, no?
    // should not be called from the outside because it's internals, the case is handled in the HybridStoreConnection when
    // the store is being merged we don't call it again..
    // starts the merging process to merge the delta into HDT
    public void makeMerge() {
        try {
            this.isMergeTriggered = true;
            logger.info("START MERGE");

            MergeRunnable mergeRunnable = new MergeRunnable(locationHdt, this);
            mergeRunnable.setExtendsTimeMergeBeginning(extendsTimeMergeBeginning);
            mergeRunnable.setExtendsTimeMergeBeginningAfterSwitch(extendsTimeMergeBeginningAfterSwitch);
            mergeRunnable.setExtendsTimeMergeEnd(extendsTimeMergeEnd);
            mergerThread = new Thread(mergeRunnable);
            mergerThread.start();
            logger.info("MERGE THREAD LUNCHED");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public HDTConverter getHdtConverter() {
        return hdtConverter;
    }

    public String getLocationNative() {
        return locationNative;
    }

    public String getLocationHdt() {
        return locationHdt;
    }

    public void setTriplesCount(long triplesCount) {
        this.triplesCount = triplesCount;
    }

    public BitArrayDisk getBitX() {
        return bitX;
    }

    public BitArrayDisk getBitY() {
        return bitY;
    }

    public BitArrayDisk getBitZ() {
        return bitZ;
    }

    public HDTSpecification getHDTSpec() {
        return spec;
    }

    public void setSpec(HDTSpecification spec) {
        this.spec = spec;
    }

    public int getExtendsTimeMergeBeginning() {
        return extendsTimeMergeBeginning;
    }

    public void setExtendsTimeMergeBeginning(int extendsTimeMerge) {
        this.extendsTimeMergeBeginning = extendsTimeMerge;
    }

    public int getExtendsTimeMergeBeginningAfterSwitch() {
        return extendsTimeMergeBeginningAfterSwitch;
    }

    public void setExtendsTimeMergeBeginningAfterSwitch(int extendsTimeMergeBeginningAfterSwitch) {
        this.extendsTimeMergeBeginningAfterSwitch = extendsTimeMergeBeginningAfterSwitch;
    }

    public int getExtendsTimeMergeEnd() {
        return extendsTimeMergeEnd;
    }

    public void setExtendsTimeMergeEnd(int extendsTimeMergeEnd) {
        this.extendsTimeMergeEnd = extendsTimeMergeEnd;
    }

}

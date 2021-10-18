package org.rdfhdt.hdt.rdf4j;

import eu.qanswer.enpoint.BitArrayDisk;
import eu.qanswer.enpoint.MergeRunnable;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.concurrent.locks.LockManager;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.AbstractValueFactoryHDT;
import org.eclipse.rdf4j.model.impl.SimpleStatement;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverClient;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractNotifyingSail;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf4j.utility.HDTProps;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class HybridStore extends AbstractNotifyingSail implements FederatedServiceResolverClient {
    private static final Logger logger = LoggerFactory.getLogger(HybridStore.class);
    private HDT hdt;

    public NativeStore nativeStoreA;
    public NativeStore nativeStoreB;
    public NativeStore currentStore;

    private BitArrayDisk deleteBitMap;
    private BitArrayDisk tempdeleteBitMap;

    private SailRepository repo;
    public boolean switchStore = false;

    public boolean isMerging = false;
    private String locationHdt;

    private int threshold;

    private boolean inMemDeletes;

    private HDTProps hdtProps;
    HDTSpecification spec;
    ValueFactory valueFactory;
    File checkFile;

    private RDFWriter rdfWriterTempTriples;
    private String locationNative;
    public LockManager manager;
    public LockManager connectionsLockManager;

    public HybridStore(String locationHdt,String locationNative,boolean inMemDeletes){

        try {
            HDTSpecification spec = new HDTSpecification();
            spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
            this.spec = spec;
            this.hdt = HDTManager.mapIndexedHDT(locationHdt+"index.hdt",spec);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.checkFile = new File(locationNative+"which_store.check");
        this.nativeStoreA = new NativeStore(new File(locationNative+"A"),"spoc,posc,cosp");
        this.nativeStoreB = new NativeStore(new File(locationNative+"B"),"spoc,posc,cosp");
        this.valueFactory = new AbstractValueFactoryHDT(hdt);
        // init the store before creating the check store file
        this.nativeStoreA.init();
        this.nativeStoreB.init();
        checkWhichStore();
        if(switchStore)
            this.currentStore = nativeStoreB;
        else
            this.currentStore = nativeStoreA;
        this.currentStore.init();
        this.threshold = 100000;

        this.repo = new SailRepository(currentStore);
        this.locationHdt = locationHdt;

        this.inMemDeletes = inMemDeletes;
        this.hdtProps = new HDTProps(this.hdt);
        this.locationNative = locationNative;
        this.manager = new LockManager();
        this.connectionsLockManager = new LockManager();
        initDeleteArray();
    }
    public HybridStore(HDT hdt,String locationHdt,String locationNative,boolean inMemDeletes){
        this.hdt = hdt;
        this.spec = new HDTSpecification();
        this.spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
        this.nativeStoreA = new NativeStore(new File(locationNative+"A"),"spoc,posc,cosp");
        this.nativeStoreB = new NativeStore(new File(locationNative+"B"),"spoc,posc,cosp");
        this.valueFactory = new AbstractValueFactoryHDT(hdt);
        this.checkFile = new File(locationNative+"which_store.check");
        // init the store before creating the check store file
        this.nativeStoreA.init();
        this.nativeStoreB.init();
        checkWhichStore();
        if(switchStore)
            this.currentStore = nativeStoreB;
        else
            this.currentStore = nativeStoreA;
        this.currentStore.init();
        this.threshold = 100000;

        this.repo = new SailRepository(currentStore);
        this.locationHdt = locationHdt;

        this.inMemDeletes = inMemDeletes;
        this.hdtProps = new HDTProps(this.hdt);
        this.locationNative = locationNative;
        this.manager = new LockManager();
        this.connectionsLockManager = new LockManager();
        initDeleteArray();
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    public void initDeleteArray(){
        if(this.inMemDeletes)
            this.deleteBitMap = new BitArrayDisk(this.hdt.getTriples().getNumberOfElements(), true);
        else
            this.deleteBitMap = new BitArrayDisk(this.hdt.getTriples().getNumberOfElements(),this.locationHdt+"triples-delete.arr");
    }
    @Override
    protected void initializeInternal() throws SailException {
//        this.repo.init();
        this.nativeStoreA.init();
        this.nativeStoreB.init();
    }
    
    public void checkWhichStore(){
        if(!checkFile.exists() || !checkFile.isFile()){
            // file does not exist, so this is the first time running the program.
            try {
                Files.createFile(checkFile.toPath());
                Files.writeString(checkFile.toPath(),"false");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // This file exists, we already ran the program previously, just read the value
            try {
                String s = Files.readString(checkFile.toPath());
                if(s.equals("false")){
                    this.switchStore = false;
                }else if(s.equals("true")){
                    this.switchStore = true;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public void writeWhichStore(){
        try {
            if(switchStore)
                Files.writeString(checkFile.toPath(),"true");
            else
                Files.writeString(checkFile.toPath(),"false");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public int getThreshold() {
        return threshold;
    }

    public NativeStore getCurrentStore() {
        return currentStore;
    }


    public HDT getHdt() {
        return hdt;
    }

    public void setHdt(HDT hdt) {
        this.hdt = hdt;
    }


    @Override
    protected void shutDownInternal() throws SailException {
        this.nativeStoreA.shutDown();
        this.nativeStoreB.shutDown();
    }

    @Override
    public boolean isWritable() throws SailException {
        if(switchStore)
            return nativeStoreB.isWritable();
        else
            return nativeStoreA.isWritable();
    }

    public RepositoryConnection getRepoConnection(){
        if(switchStore)
            return new SailRepository(nativeStoreB).getConnection();
        else
            return new SailRepository(nativeStoreA).getConnection();
    }

    public SailRepository getRepository() {
        return repo;
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
    }

    @Override
    public void setFederatedServiceResolver(FederatedServiceResolver federatedServiceResolver) {
        nativeStoreA.setFederatedServiceResolver(federatedServiceResolver);
        nativeStoreB.setFederatedServiceResolver(federatedServiceResolver);
    }

    public SailConnection getConnectionNative(){
        return this.currentStore.getConnection();
    }

    public boolean isMerging() {
        return isMerging;
    }
    public NativeStore getNativeStoreA() {
        return nativeStoreA;
    }

    public NativeStore getNativeStoreB() {
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
    public void initTempDeleteArray(){
        this.tempdeleteBitMap = new BitArrayDisk(this.hdt.getTriples().getNumberOfElements(),
                this.locationHdt+"triples-delete-temp.arr");
    }
    /*
        Init temp file to store triples to be deleted from native store while merging
     */
    public void initTempDump(){
        FileOutputStream out = null;
        try {
            File file = new File(locationNative+"tempTriples.nt");
            if(!file.exists())
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
    public void resetDeleteArray(){

        BitArrayDisk tempDeleteBitMap = this.getTempDeleteBitMap();
        try {
            HDT newHdt = HDTManager.mapIndexedHDT(locationHdt+"new_index.hdt",this.spec);
            BitArrayDisk newDeleteArray = new BitArrayDisk(newHdt.getTriples().getNumberOfElements(),
                    this.locationHdt+"triples-delete-new.arr");
            for (int i = 0; i < tempDeleteBitMap.getNumbits(); i++) {
                if(tempDeleteBitMap.access(i)){ // means that a triple has been deleted during merge
                    // find the deleted triple in the old HDT index
                    TripleID tripleID = this.hdt.getTriples().find(i + 1);
                    if(tripleID.isValid()){

                        CharSequence subject = this.hdt.getDictionary().idToString(tripleID.getSubject(), TripleComponentRole.SUBJECT);
                        CharSequence predicate = this.hdt.getDictionary().idToString(tripleID.getPredicate(), TripleComponentRole.PREDICATE);
                        CharSequence object = this.hdt.getDictionary().idToString(tripleID.getObject(), TripleComponentRole.OBJECT);

                        IteratorTripleString hit = newHdt.search(subject, predicate, object);
                        if(hit.hasNext()){
                            TripleString next = hit.next();
                            long newIndex = next.getIndex();
                            if(newIndex > 0 )
                                newDeleteArray.set(newIndex - 1,true);
                        }
                    }
                }
            }
            newDeleteArray.force(true);
            newDeleteArray.close();
            File file = new File(this.locationHdt+"triples-delete-new.arr");
            boolean renamed = file.renameTo(new File(this.locationHdt+"triples-delete.arr"));
            if(renamed) {
                //System.out.println("Before: "+this.deleteBitMap);
                this.setDeleteBitMap(new BitArrayDisk(newHdt.getTriples().getNumberOfElements(),
                        this.locationHdt + "triples-delete.arr"));
                //System.out.println("After: "+this.deleteBitMap);
                try {
                    Files.deleteIfExists(Paths.get(this.locationHdt+"triples-delete-temp.arr"));
                    Files.deleteIfExists(Paths.get(this.locationHdt+"triples-delete-new.arr"));
                    Files.deleteIfExists(Paths.get(locationHdt+"index.hdt"));
                    Files.deleteIfExists(Paths.get(locationHdt+"index.hdt.index.v1-1"));
                    File newHDTFile = new File(locationHdt+"new_index.hdt");
                    boolean renameNew = newHDTFile.renameTo(new File(locationHdt+"index.hdt"));
                    File indexFile = new File(locationHdt+"new_index.hdt.index.v1-1");
                    boolean renamedIndex = indexFile.renameTo(new File(locationHdt+"index.hdt.index.v1-1"));
                    if(renameNew && renamedIndex) {
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
            InputStream inputStream = new FileInputStream(locationNative+"tempTriples.nt");
            RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
            rdfParser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
            try (GraphQueryResult res = QueryResults.parseGraphBackground(inputStream, null,rdfParser)) {
                while (res.hasNext()) {
                    Statement st = res.next();
                    IteratorTripleString search = this.hdt.search(st.getSubject().toString(), st.getPredicate().toString(), st.getObject().toString());
                    if(search.hasNext()){
                        TripleString next = search.next();
                        long index = next.getIndex();
                        if(index > 0 )
                            this.deleteBitMap.set(index - 1,true);
                    }
                }
            }
            catch (RDF4JException | NotFoundException e) {
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
    public String makeMerge() {
        try {
            // create a lock so that new incoming connections don't do anything
            Lock lock = this.manager.createLock("Merge-Lock");
            MergeRunnable mergeRunnable = new MergeRunnable(locationHdt,this,lock);
            Thread thread = new Thread(mergeRunnable);
            thread.start();
//            mergeRunnable.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "Merged!";
    }

    public String getLocationNative() {
        return locationNative;
    }

    public String getLocationHdt() {
        return locationHdt;
    }
}

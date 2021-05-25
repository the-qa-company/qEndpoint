package org.rdfhdt.hdt.rdf4j;

import eu.qanswer.enpoint.BitArrayDisk;
import eu.qanswer.enpoint.MergeRunnable;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.AbstractValueFactoryHDT;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverClient;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractNotifyingSail;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf4j.utility.HDTProps;

import java.io.File;
import java.io.IOException;

public class HybridStore extends AbstractNotifyingSail implements FederatedServiceResolverClient {
    private HDT hdt;

    private NativeStore nativeStoreA;
    private NativeStore nativeStoreB;
    private NativeStore currentStore;

    private BitArrayDisk deleteBitMap;

    private SailRepository repo;
    public boolean switchStore = false;

    public boolean isMerging = false;
    private String locationHdt;

    private int threshold;

    private boolean inMemDeletes;

    private HDTProps hdtProps;

    ValueFactory valueFactory;

    public HybridStore(String locationHdt,String locationNative,boolean inMemDeletes){

        try {
            HDTSpecification spec = new HDTSpecification();
            //spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");

            this.hdt = HDTManager.mapIndexedHDT(locationHdt+"index.hdt",spec);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.nativeStoreA = new NativeStore(new File(locationNative+"A"),"spoc,posc,cosp");
        this.nativeStoreB = new NativeStore(new File(locationNative+"B"),"spoc,posc,cosp");
        this.valueFactory = new AbstractValueFactoryHDT(hdt);
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
        initDeleteArray();
    }
    public HybridStore(HDT hdt,String locationNative,boolean inMemDeletes){
        this.hdt = hdt;
        this.nativeStoreA = new NativeStore(new File(locationNative+"A"),"spoc,posc,cosp");
        this.nativeStoreB = new NativeStore(new File(locationNative+"B"),"spoc,posc,cosp");
        this.valueFactory = new AbstractValueFactoryHDT(hdt);
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

    public String makeMerge() {
        try {
            MergeRunnable mergeRunnable = new MergeRunnable(locationHdt,this);
            Thread thread = new Thread(mergeRunnable);
            thread.start();
//            mergeRunnable.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(switchStore){
            this.currentStore = this.nativeStoreA;
            switchStore = false;
        }else{
            this.currentStore = this.nativeStoreB;
            switchStore = true;
        }
        return "Merged!";
    }
}

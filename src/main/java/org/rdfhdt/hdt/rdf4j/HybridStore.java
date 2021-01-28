package org.rdfhdt.hdt.rdf4j;

import eu.qanswer.enpoint.BitArrayDisk;
import eu.qanswer.enpoint.MergeRunnable;
import org.eclipse.rdf4j.model.ValueFactory;
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

import java.io.File;
import java.io.IOException;

public class HybridStore extends AbstractNotifyingSail implements FederatedServiceResolverClient {
    private HDT hdt;
    private HybridTripleSource tripleSource;
    private NativeStore nativeStoreA;
    private NativeStore nativeStoreB;
    private NativeStore currentStore;
    private NativeStore deleteStore;
    private SailConnection nativeStoreConnection;
    private SailConnection deleteStoreConnection;


    private BitArrayDisk deleteBitMap;

    private SailRepository repo;
    public boolean switchStore = true;

    public boolean isMerging = false;
    private String locationHdt;
    private HybridQueryPreparer queryPreparer;
    private int threshold;

    private boolean inMemDeletes;
    public HybridStore(NativeStore nativeStoreA,NativeStore nativeStoreB,HDT hdt,int threshold){
        this.hdt = hdt;
        this.nativeStoreA = nativeStoreA;
        this.nativeStoreB = nativeStoreB;
        this.currentStore = nativeStoreA;
        this.threshold = threshold;
        this.tripleSource = new HybridTripleSource(hdt,this);
        this.nativeStoreConnection = this.currentStore.getConnection();
        this.repo = new SailRepository(currentStore);
    }
    public HybridStore(NativeStore nativeStoreA,NativeStore nativeStoreB,NativeStore deleteStore,
                       HDT hdt,String locationHdt,int threshold,boolean inMemDeletes){
        this.hdt = hdt;
        this.nativeStoreA = nativeStoreA;
        this.nativeStoreB = nativeStoreB;
        if(switchStore)
            this.currentStore = nativeStoreB;
        else
            this.currentStore = nativeStoreA;
        this.threshold = threshold;
        this.tripleSource = new HybridTripleSource(hdt,this);
        this.nativeStoreConnection = this.currentStore.getConnection();
        this.repo = new SailRepository(currentStore);
        this.locationHdt = locationHdt;
        this.queryPreparer = new HybridQueryPreparer(this);
        this.deleteStore = deleteStore;
        this.deleteStoreConnection = this.deleteStore.getConnection();
        this.inMemDeletes = inMemDeletes;
        initDeleteArray();
    }

    public HybridStore(String locationHdt,String locationNative,boolean inMemDeletes){

        try {
            this.hdt = HDTManager.mapIndexedHDT(locationHdt+"index.hdt",new HDTSpecification());
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.nativeStoreA = new NativeStore(new File(locationNative+"A"),"spoc,posc,cosp");
        this.nativeStoreB = new NativeStore(new File(locationNative+"B"),"spoc,posc,cosp");
        if(switchStore)
            this.currentStore = nativeStoreB;
        else
            this.currentStore = nativeStoreA;
        this.threshold = 100000;
        this.tripleSource = new HybridTripleSource(hdt,this);
        this.nativeStoreConnection = this.currentStore.getConnection();
        this.repo = new SailRepository(currentStore);
        this.locationHdt = locationHdt;
        this.queryPreparer = new HybridQueryPreparer(this);
        this.deleteStore = new NativeStore(new File(locationNative+"delete"),"spoc,posc,cosp");
        this.deleteStoreConnection = this.deleteStore.getConnection();
        this.inMemDeletes = inMemDeletes;
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
        nativeStoreA.init();
        nativeStoreB.init();
    }

    public int getThreshold() {
        return threshold;
    }

    public NativeStore getCurrentStore() {
        return currentStore;
    }
    public SailConnection getNativeStoreConnection(){
        return this.nativeStoreConnection;
    }

    public void setNativeStoreConnection(SailConnection nativeStoreConnection) {
        this.nativeStoreConnection = nativeStoreConnection;
    }

    public HDT getHdt() {
        return hdt;
    }

    public void setHdt(HDT hdt) {
        this.hdt = hdt;
    }

    public HybridTripleSource getTripleSource() {
        return tripleSource;
    }

    @Override
    protected void shutDownInternal() throws SailException {
        nativeStoreA.shutDown();
        nativeStoreB.shutDown();
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
    public RepositoryConnection getDeleteRepoConnection(){
        return new SailRepository(deleteStore).getConnection();
    }

    @Override
    public ValueFactory getValueFactory() {
        if(nativeStoreA == null)
            System.out.println("A is null");
        else if(nativeStoreB == null)
            System.out.println("B is null");
        if(switchStore)
            return nativeStoreB.getValueFactory();
        else
            return nativeStoreA.getValueFactory();
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
    public void setFederatedServiceResolver(FederatedServiceResolver federatedServiceResolver) {
        nativeStoreA.setFederatedServiceResolver(federatedServiceResolver);
        nativeStoreB.setFederatedServiceResolver(federatedServiceResolver);
    }

    public HybridQueryPreparer getQueryPreparer() {
        return queryPreparer;
    }

    public void setQueryPreparer(HybridQueryPreparer queryPreparer) {
        this.queryPreparer = queryPreparer;
    }


    public SailConnection getDeleteStoreConnection() {
        return deleteStoreConnection;
    }

    public NativeStore getDeleteStore() {
        return deleteStore;
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


    public void setTripleSource(HybridTripleSource tripleSource) {
        this.tripleSource = tripleSource;
    }


    public BitArrayDisk getDeleteBitMap() {
        return deleteBitMap;
    }

    public String makeMerge() {
        try {
            MergeRunnable mergeRunnable = new MergeRunnable(locationHdt,this);
            Thread thread = new Thread(mergeRunnable);
            thread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(switchStore){
            this.setNativeStoreConnection(this.nativeStoreA.getConnection());
            this.currentStore = this.nativeStoreA;
            switchStore = false;
        }else{
            this.setNativeStoreConnection(this.nativeStoreB.getConnection());
            this.currentStore = this.nativeStoreB;
            switchStore = true;
        }
        return "Merged!";
    }
}

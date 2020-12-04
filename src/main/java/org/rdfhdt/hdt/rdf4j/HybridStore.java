package org.rdfhdt.hdt.rdf4j;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverClient;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailStore;
import org.eclipse.rdf4j.sail.helpers.AbstractNotifyingSail;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.hdt.HDT;

public class HybridStore extends AbstractNotifyingSail implements FederatedServiceResolverClient {
    private HDT hdt;
    private HybridTripleSource tripleSource;
    private NativeStore nativeStoreA;
    private NativeStore nativeStoreB;
    private NativeStore currentStore;

    private SailConnection nativeStoreConnection;

    private SailRepository repo;
    public boolean switchStore = false;
    public HybridStore(NativeStore nativeStoreA,NativeStore nativeStoreB,HDT hdt){
        this.hdt = hdt;
        this.nativeStoreA = nativeStoreA;
        this.nativeStoreB = nativeStoreB;
        this.currentStore = nativeStoreA;
        this.tripleSource = new HybridTripleSource(hdt,this);
        this.nativeStoreConnection = this.currentStore.getConnection();
        this.repo = new SailRepository(currentStore);
    }

    @Override
    protected void initializeInternal() throws SailException {
        nativeStoreA.init();
        nativeStoreB.init();
    }

    public NativeStore getCurrentStore() {
        return currentStore;
    }
    public SailConnection getNativeStoreConnection(){
        return this.nativeStoreConnection;
    }

    public HDT getHdt() {
        return hdt;
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

    public int getCurrentCount(){
        String queryCount = "select (count(*) as ?c) where { ?s ?p ?o}";

        TupleQuery tupleQuery = repo.getConnection().prepareTupleQuery(queryCount);
        try (TupleQueryResult result = tupleQuery.evaluate()) {
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                Value valueOfC = bindingSet.getValue("c");
                return Integer.parseInt(valueOfC.stringValue());
            }
        }
        return 0;
    }
    public NativeStore getNativeStoreA() {
        return nativeStoreA;
    }

    public NativeStore getNativeStoreB() {
        return nativeStoreB;
    }

//    public SailStore getSailStore() {
//        return this.store;
//    }
}

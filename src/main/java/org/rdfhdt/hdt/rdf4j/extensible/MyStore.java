package org.rdfhdt.hdt.rdf4j.extensible;

import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.extensiblestore.ExtensibleStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.hdt.HDT;

public class MyStore extends ExtensibleStore<MyDataStructure,MyNameSpaceStore> {

    NativeStore nativeStore;

    public MyStore(NativeStore nativeStore, HDT hdt){
        this.nativeStore = nativeStore;
        dataStructure = new MyDataStructure(nativeStore,hdt);
        namespaceStore = new MyNameSpaceStore();
    }

    @Override
    protected synchronized void initializeInternal() throws SailException {
        this.nativeStore.initialize();
        super.initializeInternal();
    }

    @Override
    protected synchronized void shutDownInternal() throws SailException {
        this.nativeStore.shutDown();
    }

    @Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new MyStoreConnection(this);
    }


    public boolean isWritable() throws SailException {
        return true;
    }
}

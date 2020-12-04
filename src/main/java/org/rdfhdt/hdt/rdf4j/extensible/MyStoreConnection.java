package org.rdfhdt.hdt.rdf4j.extensible;

import org.eclipse.rdf4j.sail.extensiblestore.ExtensibleStoreConnection;

public class MyStoreConnection extends ExtensibleStoreConnection<MyStore> {

    MyStoreConnection(MyStore sail) {
        super(sail);
    }

}

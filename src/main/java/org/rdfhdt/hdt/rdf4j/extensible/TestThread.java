package org.rdfhdt.hdt.rdf4j.extensible;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

import java.io.File;

public class TestThread {

    public static void main(String[] args) {
        File dataDir1 = new File("/Users/alyhdr/Desktop/qa-company/hdtsparqlendpoint/native-store/A/");
        File dataDir2 = new File("/Users/alyhdr/Desktop/qa-company/hdtsparqlendpoint/native-store/B/");

        String indexes = "spoc,posc,cosp";

        NativeStore nativeStore1 = new NativeStore(dataDir1,indexes);
        SailConnection connection1 = nativeStore1.getConnection();
        connection1.begin();
        connection1.clear((Resource)null);
        connection1.commit();

        NativeStore nativeStore2 = new NativeStore(dataDir2,indexes);
        SailConnection connection2 = nativeStore2.getConnection();
        connection2.begin();
        connection2.clear((Resource)null);
        connection2.commit();
    }
}

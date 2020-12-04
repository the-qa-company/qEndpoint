package org.rdfhdt.hdt.rdf4j.extensible;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.io.File;
import java.io.IOException;

public class Test {
    public static void main(String[] args) {

        HDTSpecification spec = new HDTSpecification();
        String location = "/Users/alyhdr/Desktop/qa-company/hdtsparqlendpoint/";
        try {
            HDT hdt = HDTManager.mapIndexedHDT(
                    new File(location+"/hdt-store/index.hdt").getAbsolutePath(),spec);
            File dataDir = new File(location+"/test-native");
            String indexes = "spoc,posc,cosp";
            NativeStore nativeStore = new NativeStore(dataDir,indexes);
            MyStore myStore = new MyStore(nativeStore,hdt);
            RepositoryConnection connection = new SailRepository(myStore).getConnection();
            RepositoryResult<Statement> statements = connection.getStatements(null, null, null, false, (Resource) null);
            while (statements.hasNext()){
                System.out.println(statements.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

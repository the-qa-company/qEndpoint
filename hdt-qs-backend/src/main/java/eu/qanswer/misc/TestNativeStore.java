package eu.qanswer.misc;

import eu.qanswer.hybridstore.HybridStore;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.io.File;
import java.io.IOException;

public class TestNativeStore {
    private NativeStore nativeStore;
    private HDT hdt;
    public TestNativeStore(HDT hdt){
        this.hdt = hdt;
        nativeStore = new NativeStore(new File("/Users/alyhdr/Desktop/qa-company/data/admin/eu/native-store/A"),"spoc,posc,cosp");
    }
    public static void main(String[] args) {

//        NativeStore nativeStore = new NativeStore(new File("/Users/alyhdr/Desktop/qa-company/hdt-query-service/hdt-qs-backend/native-store/A"), "spoc,posc,cosp");


        HDTSpecification spec = new HDTSpecification();
        spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
        HybridStore nativeStore = null;
        try {
            nativeStore = new HybridStore("/Users/alyhdr/Desktop/qa-company/hdt-query-service/hdt-qs-backend/hdt-store/",
                    spec,"/Users/alyhdr/Desktop/qa-company/hdt-query-service/hdt-qs-backend/native-store/",false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SailRepository repository = new SailRepository(nativeStore);

        String ex = "http://hdt.org/";
        int numbeOfTriples = 1300;
        String sparqlQuery = "INSERT DATA { ";
        for (int i=0; i<numbeOfTriples; i++){
            sparqlQuery += "<http://s"+i+">  <http://p"+i+">  <http://o"+i+"> . ";
        }
        sparqlQuery += "} ";
        RepositoryConnection connection = repository.getConnection();
        Update tupleQuery = connection.prepareUpdate(sparqlQuery);
        tupleQuery.execute();

        sparqlQuery = "SELECT ?s WHERE { ?s  <http://p1> <http://o1> . } ";
        TupleQuery tupleQuery1 = connection.prepareTupleQuery(sparqlQuery);
        TupleQueryResult tupleQueryResult = tupleQuery1.evaluate();
        // print out the result
        tupleQueryResult.stream().iterator().forEachRemaining(System.out::println);

        connection.begin();
        connection.clear();
        connection.commit();
    }
//    private void writeTempFile(RepositoryConnection connection,String file){
//        FileOutputStream out = null;
//        try {
//            out = new FileOutputStream(file);
//            RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
//            RepositoryResult<Statement> repositoryResult =
//                    connection.getStatements(null,null,null,false);
//            writer.startRDF();
//            IRIConverter iriConverter = new IRIConverter(this.hdt);
//            while (repositoryResult.hasNext()) {
//                Statement stm = repositoryResult.next();
//                Statement stmConverted = this.hybridStore.getValueFactory().createStatement(
//                        iriConverter.getIRIHdtSubj(stm.getSubject()),
//                        iriConverter.getIRIHdtPred(stm.getPredicate()),
//                        iriConverter.getIRIHdtObj(stm.getObject())
//                );
//                writer.handleStatement(stmConverted);
//            }
//            writer.endRDF();
//            out.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

}

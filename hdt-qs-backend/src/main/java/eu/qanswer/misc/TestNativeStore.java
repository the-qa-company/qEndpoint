package eu.qanswer.misc;

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
        try {
            HDTSpecification spec = new HDTSpecification();
            spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
            HDT hdt = HDTManager.mapIndexedHDT("/Users/alyhdr/Desktop/qa-company/data/admin/eu-2/hdt_index/index.hdt",spec);
            System.out.println(hdt.getDictionary().getNshared() + hdt.getDictionary().getNAllObjects());

            System.out.println(hdt.getDictionary().idToString(21980853,TripleComponentRole.OBJECT));

        } catch (IOException e) {
            e.printStackTrace();
        }
//        NativeStore nativeStore = new NativeStore(new File("/Users/alyhdr/Desktop/qa-company/hdt-query-service/hdt-qs-backend/native-store/B"), "spoc,posc,cosp");
//        SailRepository repository = new SailRepository(nativeStore);
//        ValueFactory vf = new MemValueFactory();
//        String ex = "http://hdt.org/";
//        try (RepositoryConnection connection = repository.getConnection()) {
//            long size = connection.size();
//            System.out.println(size);
//            RepositoryResult<Statement> statements = connection.getStatements(null, null, null,false);
//
//            for (Statement s:statements) {
//                System.out.println(s);
//            }
//        }

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

package eu.qanswer.enpoint;

import org.eclipse.rdf4j.http.client.RDF4JProtocolSession;
import org.eclipse.rdf4j.http.client.SharedHttpClientSessionManager;
import org.eclipse.rdf4j.http.protocol.Protocol;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf4j.HybridStore;
import org.rdfhdt.hdt.rdf4j.utility.IRIConverter;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestNativeStore {
    private NativeStore nativeStore;
    private HDT hdt;
    public TestNativeStore(HDT hdt){
        this.hdt = hdt;
        nativeStore = new NativeStore(new File("/Users/alyhdr/Desktop/qa-company/data/admin/eu/native-store/A"),"spoc,posc,cosp");
    }
    public static void main(String[] args) {
//        try {
//            HDTSpecification spec = new HDTSpecification();
//            spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
//            HDT hdt = HDTManager.mapIndexedHDT("/Users/alyhdr/Desktop/qa-company/data/admin/eu/hdt_index/index.hdt",spec);
//            CharSequence charSequence = hdt.getDictionary().idToString(-110, TripleComponentRole.OBJECT);
//            System.out.println(charSequence);
////            IteratorTripleString search = hdt.search("", "", "");
////            while (search.hasNext()){
////                System.out.println(search.next());
////            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        NativeStore nativeStore = new NativeStore(new File("/Users/alyhdr/Desktop/qa-company/data/admin/eu/native-store/B"), "spoc,posc,cosp");
        SailRepository repository = new SailRepository(nativeStore);
        ValueFactory vf = new MemValueFactory();
        String ex = "http://hdt.org/";
        try (RepositoryConnection connection = repository.getConnection()) {
            long size = connection.size();
            System.out.println(size);
//            RepositoryResult<Statement> statements = connection.getStatements(vf.createIRI("https://linkedopendata.eu/entity/","Q16"), null, null);
//
//            for (Statement s:statements) {
//                System.out.println(s);
//            }
        }

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

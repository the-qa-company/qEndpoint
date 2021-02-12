package eu.qanswer.enpoint;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestNativeStore {

    private static final Logger logger = LoggerFactory.getLogger(TestNativeStore.class);

    public static void main(String[] args) {

        NativeStore nativeStoreA = new NativeStore(new File("/Users/alyhdr/Desktop/qa-company/hdtsparqlendpoint/native-store/B"),"spoc,posc,cosp");
        SailRepository repository = new SailRepository(nativeStoreA);
        writeTempFile(repository.getConnection(),"/Users/alyhdr/Desktop/index.nt");
        String rdfInput = "/Users/alyhdr/Desktop/index.nt";
        String hdtOutput = "/Users/alyhdr/Desktop/index.hdt";
        String baseURI = "file://"+rdfInput;
        RDFNotation notation = RDFNotation.guess(rdfInput);
        HDTSpecification spec = new HDTSpecification();

        try {
            StopWatch sw = new StopWatch();
            HDT hdt = HDTManager.generateHDT(new File(rdfInput).getAbsolutePath(), baseURI,RDFNotation.NTRIPLES , spec, null);
            logger.info("File converted in: "+sw.stopAndShow());
            hdt.saveToHDT(hdtOutput, null);
            logger.info("HDT saved to file in: "+sw.stopAndShow());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserException e) {
            e.printStackTrace();
        }
    }
    private static void writeTempFile(RepositoryConnection connection, String file){
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
            RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
            RepositoryResult<Statement> repositoryResult =
                    connection.getStatements(null,null,null,false,(Resource)null);
            writer.startRDF();
            while (repositoryResult.hasNext()) {
                Statement stm = repositoryResult.next();
                writer.handleStatement(stm);
            }
            writer.endRDF();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

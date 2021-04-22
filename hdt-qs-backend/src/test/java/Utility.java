import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class Utility {


    public static HDT createTempHdtIndex(TemporaryFolder tempDir,boolean empty, boolean isBig){
        try {
            String rdfInput = "temp.nt";
            File inputFile = tempDir.newFile();
            if(!empty){
                if(!isBig)
                    writeTempRDF(inputFile);
                else
                    writeBigIndex(inputFile);
            }
            String baseURI = inputFile.getAbsolutePath();
            RDFNotation notation = RDFNotation.guess(rdfInput);
            HDTSpecification spec = new HDTSpecification();
            spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
            HDT hdt = HDTManager.generateHDT(inputFile.getAbsolutePath(),baseURI,notation,spec,null);
            return HDTManager.indexedHDT(hdt,null);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static int count = 1000000;
    private static void writeBigIndex(File file){
        FileOutputStream out = null;
        ValueFactory vf = new MemValueFactory();
        try {
            out = new FileOutputStream(file);
            RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
            writer.startRDF();
            String ex = "http://example.com/";
            for (int i = 1; i <= count ; i++) {
                IRI entity = vf.createIRI(ex,"person"+i);
                Statement stm = vf.createStatement(entity, RDF.TYPE, FOAF.PERSON);
                writer.handleStatement(stm);
            }
            writer.endRDF();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void writeTempRDF(File file){
        FileOutputStream out = null;
        ValueFactory vf = new MemValueFactory();
        try {
            out = new FileOutputStream(file);
            RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
            writer.startRDF();
            String ex = "http://example.com/";
            IRI guo = vf.createIRI(ex,"Guo");
            Statement stm = vf.createStatement(guo,RDF.TYPE,FOAF.PERSON);
            writer.handleStatement(stm);
            writer.endRDF();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


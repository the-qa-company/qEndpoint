package com.the_qa_company.q_endpoint.hybridstore;

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

    public static HDT createTempHdtIndex(TemporaryFolder fileName, boolean empty, boolean isBig, HDTSpecification spec) throws IOException {
        return createTempHdtIndex(new File(fileName.newFile() + ".nt").getAbsolutePath(), empty, isBig, spec);
    }


    public static HDT createTempHdtIndex(String fileName, boolean empty, boolean isBig, HDTSpecification spec) {
        try {
            File inputFile = new File(fileName);
            if (!empty) {
                if (!isBig)
                    writeTempRDF(inputFile);
                else
                    writeBigIndex(inputFile);
            } else {
                inputFile.createNewFile();
            }
            String baseURI = inputFile.getAbsolutePath();
            RDFNotation notation = RDFNotation.guess(fileName);
            HDT hdt = HDTManager.generateHDT(inputFile.getAbsolutePath(), baseURI, notation, spec, null);
            return HDTManager.indexedHDT(hdt, null);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static final int SUBJECTS = 1000;
    public static final int PREDICATES = 1000;
    public static final int OBJECTS = 100;
    public static final int COUNT = SUBJECTS * PREDICATES;
    public static final String EXAMPLE_NAMESPACE = "http://example.com/";

    /**
     * create a statement of a fake person (ex:personX a foaf:person)
     * @param vf the value factory
     * @param id the id of the fake person
     * @return the statement
     */
    public static Statement getFakePersonStatement(ValueFactory vf, int id) {
        IRI entity = vf.createIRI(EXAMPLE_NAMESPACE, "person" + id);
        return vf.createStatement(entity, RDF.TYPE, FOAF.PERSON);
    }

    /**
     * create a fake statement of a complete graph
     * @param vf the value factory
     * @param id the id, must be between 0 (included) and subjects*predicates*objects (excluded)
     * @return the statement
     */
    public static Statement getFakeStatement(ValueFactory vf, int id) {
        // return getFakePersonStatement(vf, id); /*
        int x = id % SUBJECTS;
        int y = (id / SUBJECTS) % PREDICATES;
        int z = id % OBJECTS;

        IRI r = vf.createIRI(EXAMPLE_NAMESPACE, "sub" + x);
        IRI p = vf.createIRI(EXAMPLE_NAMESPACE, "pre" + y);
        IRI o = vf.createIRI(EXAMPLE_NAMESPACE, "obj" + z);

        return vf.createStatement(r, p, o);
        //*/
    }

    private static void writeBigIndex(File file) {
        FileOutputStream out = null;
        ValueFactory vf = new MemValueFactory();
        try {
            out = new FileOutputStream(file);
            RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
            writer.startRDF();
            for (int i = 1; i <= COUNT; i++) {
                writer.handleStatement(getFakeStatement(vf, i - 1));
            }
            writer.endRDF();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeTempRDF(File file) {
        FileOutputStream out = null;
        ValueFactory vf = new MemValueFactory();
        try {
            out = new FileOutputStream(file);
            RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
            writer.startRDF();
            String ex = "http://example.com/";
            IRI guo = vf.createIRI(ex, "Guo");
            Statement stm = vf.createStatement(guo, RDF.TYPE, FOAF.PERSON);
            writer.handleStatement(stm);
            writer.endRDF();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


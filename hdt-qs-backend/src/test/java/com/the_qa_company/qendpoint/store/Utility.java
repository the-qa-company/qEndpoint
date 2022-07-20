package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.controller.Sparql;
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
import org.rdfhdt.hdt.options.HDTOptions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class Utility {

	/**
	 * create a temporary HDT Index
	 *
	 * @param fileName the temporaryFolder to create the temp file
	 * @param empty    if the HDT should be empty?
	 * @param isBig    if the HDT should be big?
	 * @param spec     the HDTOptions to put to the HDT
	 * @return HDT
	 * @throws IOException io error
	 */
	public static HDT createTempHdtIndex(TemporaryFolder fileName, boolean empty, boolean isBig, HDTOptions spec)
			throws IOException {
		return createTempHdtIndex(new File(fileName.newFile() + ".nt").getAbsolutePath(), empty, isBig, spec);
	}

	/**
	 * create a temporary HDT Index
	 *
	 * @param fileName the temp file name to create the temp rdf file
	 * @param empty    if the HDT should be empty?
	 * @param isBig    if the HDT should be big?
	 * @param spec     the HDTOptions to put to the HDT
	 * @return HDT
	 * @throws IOException io error
	 */
	public static HDT createTempHdtIndex(String fileName, boolean empty, boolean isBig, HDTOptions spec)
			throws IOException {
		File inputFile = new File(fileName);
		if (!empty) {
			if (!isBig)
				writeTempRDF(inputFile);
			else
				writeBigIndex(inputFile);
		} else {
			if (!inputFile.createNewFile()) {
				throw new IOException("Can't create new empty file for hdt " + fileName);
			}
		}
		String baseURI = Sparql.baseURIFromFilename(fileName);
		RDFNotation notation = RDFNotation.guess(fileName);
		HDT hdt;
		try {
			hdt = HDTManager.generateHDT(inputFile.getAbsolutePath(), baseURI, notation, spec, null);
		} catch (ParserException e) {
			throw new IOException("Can't generate hdt", e);
		}
		return HDTManager.indexedHDT(hdt, null);
	}

	/**
	 * the number of subjects for big hdt
	 */
	public static final int SUBJECTS = 1000;
	/**
	 * the number of predicates for big hdt
	 */
	public static final int PREDICATES = 1000;
	/**
	 * the number of objects for big hdt
	 */
	public static final int OBJECTS = 100;
	/**
	 * the number of triples for big hdt
	 */
	public static final int COUNT = SUBJECTS * PREDICATES;
	/**
	 * the number ex: namespace
	 */
	public static final String EXAMPLE_NAMESPACE = "http://example.com/";

	/**
	 * create a statement of a fake person (ex:personX a foaf:person)
	 *
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
	 *
	 * @param vf the value factory
	 * @param id the id, must be between 0 (included) and
	 *           subjects*predicates*objects (excluded)
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
		// */
	}

	private static void writeBigIndex(File file) throws IOException {
		ValueFactory vf = new MemValueFactory();
		try (FileOutputStream out = new FileOutputStream(file)) {
			RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
			writer.startRDF();
			for (int i = 1; i <= COUNT; i++) {
				writer.handleStatement(getFakeStatement(vf, i - 1));
			}
			writer.endRDF();
		}
	}

	private static void writeTempRDF(File file) throws IOException {
		ValueFactory vf = new MemValueFactory();
		try (FileOutputStream out = new FileOutputStream(file)) {
			RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
			writer.startRDF();
			String ex = "http://example.com/";
			IRI guo = vf.createIRI(ex, "Guo");
			Statement stm = vf.createStatement(guo, RDF.TYPE, FOAF.PERSON);
			writer.handleStatement(stm);
			writer.endRDF();
		}
	}
}

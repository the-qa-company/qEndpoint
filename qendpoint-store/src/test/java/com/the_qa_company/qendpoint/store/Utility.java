package com.the_qa_company.qendpoint.store;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.base.RepositoryConnectionWrapper;
import org.eclipse.rdf4j.repository.base.RepositoryWrapper;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.helpers.SailConnectionWrapper;
import org.eclipse.rdf4j.sail.helpers.SailWrapper;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.junit.rules.TemporaryFolder;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

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
		String baseURI = EndpointStoreUtils.baseURIFromFilename(fileName);
		RDFNotation notation = RDFNotation.guess(fileName);
		HDT hdt;
		try {
			hdt = HDTManager.generateHDT(inputFile.getAbsolutePath(), baseURI, notation, spec, null);
		} catch (ParserException e) {
			throw new IOException("Can't generate hdt", e);
		}
		return HDTManager.indexedHDT(hdt, null, spec);
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

	public static Sail convertToDumpSail(Sail sail) {
		return new SailWrapper(sail) {
			@Override
			public SailConnection getConnection() throws SailException {
				return new SailConnectionWrapper(super.getConnection()) {
					@Override
					public void addStatement(Resource subj, IRI pred, Value obj, Resource... contexts)
							throws SailException {
						System.out.printf("ADD : (%s, %s, %s %s)\n", subj, pred, obj, Arrays.toString(contexts));
						super.addStatement(subj, pred, obj, contexts);
					}

					@Override
					public void removeStatement(UpdateContext modify, Resource subj, IRI pred, Value obj,
							Resource... contexts) throws SailException {
						System.out.printf("REMOVE[%s] : (%s, %s, %s %s)\n", modify, subj, pred, obj,
								Arrays.toString(contexts));
						super.removeStatement(modify, subj, pred, obj, contexts);
					}

					@Override
					public void clear(Resource... contexts) throws SailException {
						System.out.printf("CLEAR %s\n", Arrays.toString(contexts));
						super.clear(contexts);
					}
				};
			}
		};
	}

	public static Repository convertToDumpRepository(Repository repository) {
		return new RepositoryWrapper(repository) {
			@Override
			public void shutDown() throws RepositoryException {
				System.out.println("SHUTDOWN REPO");
				super.shutDown();
			}

			@Override
			public RepositoryConnection getConnection() throws RepositoryException {
				return new RepositoryConnectionWrapper(this, super.getConnection()) {
					@Override
					public Query prepareQuery(QueryLanguage ql, String query, String baseURI)
							throws MalformedQueryException, RepositoryException {
						System.out.println("QUERY: " + query);
						return super.prepareQuery(ql, query, baseURI);
					}

					@Override
					public GraphQuery prepareGraphQuery(QueryLanguage ql, String query, String baseURI)
							throws RepositoryException, MalformedQueryException {
						System.out.println("GRAPH: " + query);
						return super.prepareGraphQuery(ql, query, baseURI);
					}

					@Override
					public TupleQuery prepareTupleQuery(QueryLanguage ql, String query, String baseURI)
							throws MalformedQueryException, RepositoryException {
						System.out.println("TUPLE: " + query);
						return super.prepareTupleQuery(ql, query, baseURI);
					}

					@Override
					public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query, String baseURI)
							throws MalformedQueryException, RepositoryException {
						System.out.println("UPDATE: " + query);
						return super.prepareBooleanQuery(ql, query, baseURI);
					}

					@Override
					public Update prepareUpdate(QueryLanguage ql, String update, String baseURI)
							throws MalformedQueryException, RepositoryException {
						System.out.println("UPDATE: " + update);
						return super.prepareUpdate(ql, update, baseURI);
					}
				};
			}
		};
	}
}

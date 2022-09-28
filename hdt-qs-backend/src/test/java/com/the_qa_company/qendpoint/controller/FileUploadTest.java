package com.the_qa_company.qendpoint.controller;

import com.the_qa_company.qendpoint.Application;
import com.the_qa_company.qendpoint.compiler.DebugOptionTestUtils;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.utils.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.utils.RDFStreamUtils;
import com.the_qa_company.qendpoint.compiler.SailCompilerSchema;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFParserRegistry;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestContextManager;
import org.springframework.util.FileSystemUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

@RunWith(Parameterized.class)
//@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@SpringBootTest(classes = Application.class)
@DirtiesContext
@Ignore("skip")
public class FileUploadTest {
	public static final String COKTAILS_NT = "cocktails.nt";
	private static final Logger logger = LoggerFactory.getLogger(FileUploadTest.class);

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object> params() {
		return new ArrayList<>(RDFParserRegistry.getInstance().getKeys());
	}

	@Autowired
	Sparql sparql;

	@Value("${locationHdt}")
	String locationHdt;

	@Value("${hdtIndexName}")
	String hdtIndexName;

	@Value("${locationNative}")
	String locationNative;

	private final String fileName;
	private final RDFFormat format;

	public FileUploadTest(RDFFormat format) throws IOException {
		this.format = format;
		RDFFormat originalFormat = Rio.getParserFormatForFileName(COKTAILS_NT).orElseThrow();

		RDFParser parser = Rio.createParser(originalFormat);
		Path testDir = Paths.get("tests", "testrdf");
		Files.createDirectories(testDir);
		Path RDFFile = testDir.resolve(COKTAILS_NT + "." + format.getDefaultFileExtension());
		if (!Files.exists(RDFFile)) {
			try (OutputStream os = new FileOutputStream(RDFFile.toFile()); InputStream is = stream(COKTAILS_NT)) {
				RDFWriter writer = Rio.createWriter(format, os);
				parser.setRDFHandler(noBNode(writer));
				parser.parse(is);
			}
		}

		fileName = RDFFile.toFile().getAbsolutePath();
	}

	@Before
	public void setup() throws Exception {
		// init spring runner
		TestContextManager testContextManager = new TestContextManager(getClass());
		testContextManager.prepareTestInstance(this);

		// clear map to recreate endpoint store
		sparql.init = false;

		// remove previous data
		try {
			FileSystemUtils.deleteRecursively(Paths.get(locationHdt));
		} catch (IOException e) {
			//
		}
		try {
			FileSystemUtils.deleteRecursively(Paths.get(locationNative));
		} catch (IOException e) {
			//
		}
		try {
			FileSystemUtils.deleteRecursively(Paths.get(hdtIndexName));
		} catch (IOException e) {
			//
		}
	}

	@After
	public void complete() throws IOException {
		sparql.shutdown();
	}

	private InputStream stream(String file) {
		return Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(file), "file can't be found!");
	}

	private InputStream streamOut(String file) throws FileNotFoundException {
		return new FileInputStream(file);
	}

	private long fileSize(String file) throws IOException {
		InputStream testNt = streamOut(file);
		byte[] buff = new byte[1024];

		long r;
		long size = 0;
		while ((r = testNt.read(buff)) != -1) {
			size += r;
		}
		return size;
	}

	private String clearSpaces(String text) {
		return text.matches("(\\s|[\\n\\r])*") ? "" : text;
	}

	private org.eclipse.rdf4j.model.Value clearSpaces(ValueFactory vf, org.eclipse.rdf4j.model.Value value) {
		if (!value.isLiteral()) {
			return value;
		}
		Literal lit = (Literal) value;
		IRI dt = lit.getDatatype();
		if (dt.equals(XSD.STRING)) {
			return vf.createLiteral(clearSpaces(lit.stringValue()));
		} else if (dt.equals(RDF.LANGSTRING)) {
			return vf.createLiteral(clearSpaces(lit.stringValue()), lit.getLanguage().orElseThrow());
		}
		return lit;
	}

	private void assertAllCoktailsHDTLoaded() throws IOException {
		assertAllTriplesHDTLoaded(stream(COKTAILS_NT), COKTAILS_NT);
	}

	private void assertAllTriplesHDTLoaded(InputStream stream, String fileName) throws IOException {
		EndpointStore store = sparql.endpoint;
		SailRepository sailRepository = new SailRepository(store);
		List<Statement> statementList = new ArrayList<>();
		Consumer<Statement> consumer;
		// fix because RDFXML can't handle empty spaces literals
		if (format == RDFFormat.RDFXML) {
			consumer = statement -> {
				if (statement.getSubject().isBNode() || statement.getObject().isBNode())
					return;
				org.eclipse.rdf4j.model.Value v = clearSpaces(store.getValueFactory(), statement.getObject());
				if (v != statement.getObject()) {
					statementList.add(store.getValueFactory().createStatement(statement.getSubject(),
							statement.getPredicate(), v));
				} else {
					statementList.add(statement);
				}
			};
		} else {
			consumer = statement -> {
				if (statement.getSubject().isBNode() || statement.getObject().isBNode())
					return;
				statementList.add(statement);
			};
		}
		RDFStreamUtils.readRDFStream(stream, Rio.getParserFormatForFileName(fileName).orElseThrow(), true, consumer);

		try (SailRepositoryConnection connection = sailRepository.getConnection()) {
			RepositoryResult<Statement> sts = connection.getStatements(null, null, null, false);
			while (sts.hasNext()) {
				Statement next = sts.next();
				if (next.getSubject().isBNode() || next.getObject().isBNode()
						|| next.getSubject().toString().startsWith("_:")
						|| next.getObject().toString().startsWith("_:"))
					continue;
				Assert.assertTrue("Statement (" + next.getSubject().toString() + ", " + next.getPredicate().toString()
						+ ", " + next.getObject().toString() + "), not in " + fileName, statementList.remove(next));
				while (statementList.remove(next)) {
					// remove duplicates
					logger.trace("removed duplicate of {}", next);
				}
			}
		}
		if (!statementList.isEmpty()) {
			for (Statement statement : statementList) {
				System.err.println(statement);
			}
			Assert.fail(fileName + " contains more triples than the EndpointStore");
		}
	}

	@Test
	public void loadNoSplitOnePassTest() throws IOException {
		long size = fileSize(fileName);
		sparql.debugMaxChunkSize = size + 1;
		sparql.sparqlRepository.getOptions().setPassMode(SailCompilerSchema.HDT_ONE_PASS_MODE);

		sparql.loadFile(streamOut(fileName), fileName);

		assertAllCoktailsHDTLoaded();
	}

	@Test
	public void loadSplitOnePassTest() throws IOException {
		long size = fileSize(fileName);
		sparql.debugMaxChunkSize = size / 10;
		sparql.sparqlRepository.getOptions().setPassMode(SailCompilerSchema.HDT_ONE_PASS_MODE);

		sparql.loadFile(streamOut(fileName), fileName);

		assertAllCoktailsHDTLoaded();
	}

	@Test
	public void loadNoSplitTwoPassTest() throws IOException {
		long size = fileSize(fileName);
		sparql.debugMaxChunkSize = size + 1;
		sparql.sparqlRepository.getOptions().setPassMode(SailCompilerSchema.HDT_TWO_PASS_MODE);

		sparql.loadFile(streamOut(fileName), fileName);

		assertAllCoktailsHDTLoaded();
	}

	@Test
	public void loadSplitTwoPassTest() throws IOException {
		long size = fileSize(fileName);
		sparql.debugMaxChunkSize = size / 10;
		sparql.sparqlRepository.getOptions().setPassMode(SailCompilerSchema.HDT_TWO_PASS_MODE);

		sparql.loadFile(streamOut(fileName), fileName);

		assertAllCoktailsHDTLoaded();
	}

	@Test
	@Ignore("large test")
	public void loadLargeTest() throws IOException {
		long size = Sparql.getMaxChunkSize() * 10;
		LargeFakeDataSetStreamSupplier supplier = new LargeFakeDataSetStreamSupplier(size, 42);
		sparql.loadFile(supplier.createRDFStream(format), "fake." + format.getDefaultFileExtension());

		supplier.reset();

		assertAllTriplesHDTLoaded(supplier.createRDFStream(format), "fake." + format.getDefaultFileExtension());
	}

	private RDFHandler noBNode(RDFHandler handler) {
		return new RDFHandler() {
			@Override
			public void startRDF() throws RDFHandlerException {
				handler.startRDF();
			}

			@Override
			public void endRDF() throws RDFHandlerException {
				handler.endRDF();
			}

			@Override
			public void handleNamespace(String s, String s1) throws RDFHandlerException {
				handler.handleNamespace(s, s1);
			}

			@Override
			public void handleStatement(Statement statement) throws RDFHandlerException {
				if (statement.getSubject().isBNode() || statement.getObject().isBNode()
						|| statement.getPredicate().isBNode())
					return;
				handler.handleStatement(statement);
			}

			@Override
			public void handleComment(String s) throws RDFHandlerException {
				handler.handleComment(s);
			}
		};
	}
}

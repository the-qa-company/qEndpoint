package com.the_qa_company.qendpoint.controller;

import com.the_qa_company.qendpoint.Application;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.utils.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.utils.RDFStreamUtils;
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
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
public class FileUploadTest {
	public static final String COKTAILS_NT = "cocktails.nt";
	private static final Logger logger = LoggerFactory.getLogger(FileUploadTest.class);

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object> params() {
		ArrayList<Object> list = new ArrayList<>(RDFParserRegistry.getInstance().getKeys());
		list.add(RDFFormat.HDT);
		return list;
	}

	@Autowired
	Sparql sparql;

	private final String fileName;
	private final RDFFormat format;

	public FileUploadTest(RDFFormat format) throws IOException, ParserException {
		this.format = format;
		RDFFormat originalFormat = Rio.getParserFormatForFileName(COKTAILS_NT).orElseThrow();

		RDFParser parser = Rio.createParser(originalFormat);
		Path testDir = Paths.get("tests", "testrdf");
		Files.createDirectories(testDir);
		Path RDFFile = testDir.resolve(COKTAILS_NT + "." + format.getDefaultFileExtension());
		boolean created = false;
		if (!Files.exists(RDFFile)) {
			created = true;
			try (OutputStream os = new FileOutputStream(RDFFile.toFile()); InputStream is = stream(COKTAILS_NT)) {
				if (format == RDFFormat.HDT) {
					try (HDT hdt = HDTManager.generateHDT(is, "http://example.org/#", RDFNotation.TURTLE,
							HDTOptions.empty(), ProgressListener.ignore())) {
						hdt.saveToHDT(os);
					}
				} else {
					RDFWriter writer = Rio.createWriter(format, os);
					parser.setRDFHandler(noBNode(writer));
					parser.parse(is);
				}
			}
		}

		fileName = RDFFile.toFile().getAbsolutePath();
		logger.info("Prepared test file for format {} at {} (created={})", format.getName(), fileName, created);
		logFileStats(RDFFile, "prepared input");
	}

	@Before
	public void setup() throws Exception {
		// init spring runner
		TestContextManager testContextManager = new TestContextManager(getClass());
		testContextManager.prepareTestInstance(this);

		sparql.shutdown();

		// remove previous data
		try {
			FileSystemUtils.deleteRecursively(Paths.get(sparql.locationHdt));
		} catch (IOException e) {
			//
		}
		try {
			FileSystemUtils.deleteRecursively(Paths.get(sparql.locationNative));
		} catch (IOException e) {
			//
		}
		try {
			FileSystemUtils.deleteRecursively(Paths.get(sparql.hdtIndexName));
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
		long[] expectedTotal = new long[1];
		long[] expectedSkippedBNode = new long[1];
		long[] expectedWhitespaceNormalized = new long[1];
		Consumer<Statement> consumer;
		logger.info("Asserting triples for format {} using expected file {}", format.getName(), fileName);
		// fix because RDFXML can't handle empty spaces literals
		if (format == RDFFormat.RDFXML) {
			consumer = statement -> {
				expectedTotal[0]++;
				if (statement.getSubject().isBNode() || statement.getObject().isBNode()) {
					expectedSkippedBNode[0]++;
					return;
				}
				org.eclipse.rdf4j.model.Value v = clearSpaces(store.getValueFactory(), statement.getObject());
				if (v != statement.getObject()) {
					expectedWhitespaceNormalized[0]++;
					statementList.add(store.getValueFactory().createStatement(statement.getSubject(),
							statement.getPredicate(), v));
				} else {
					statementList.add(statement);
				}
			};
		} else {
			consumer = statement -> {
				expectedTotal[0]++;
				if (statement.getSubject().isBNode() || statement.getObject().isBNode()) {
					expectedSkippedBNode[0]++;
					return;
				}
				statementList.add(statement);
			};
		}
		RDFStreamUtils.readRDFStream(stream, Rio.getParserFormatForFileName(fileName).orElseThrow(), true, consumer);
		logger.info("Expected triples read: total={}, kept={}, skippedBNode={}, normalizedWhitespace={}",
				expectedTotal[0], statementList.size(), expectedSkippedBNode[0], expectedWhitespaceNormalized[0]);

		try (SailRepositoryConnection connection = sailRepository.getConnection()) {
			long[] storeTotal = new long[1];
			long[] storeSkippedBNode = new long[1];
			long[] storeExtra = new long[1];
			long[] duplicateRemoved = new long[1];
			logger.info("Store size before scan: {}", connection.size());
			try (RepositoryResult<Statement> sts = connection.getStatements(null, null, null, false)) {
				while (sts.hasNext()) {
					Statement next = sts.next();
					if (next.getSubject().isBNode() || next.getObject().isBNode()
							|| next.getSubject().toString().startsWith("_:")
							|| next.getObject().toString().startsWith("_:")) {
						storeSkippedBNode[0]++;
						continue;
					}
					storeTotal[0]++;
					boolean removed = statementList.remove(next);
					if (!removed) {
						storeExtra[0]++;
						if (storeExtra[0] <= 5) {
							logger.warn("Store statement not in expected list: {}", next);
						}
					}
					Assert.assertTrue(
							"Statement (" + next.getSubject().toString() + ", " + next.getPredicate().toString() + ", "
									+ next.getObject().toString() + "), not in " + fileName,
							removed);
					if (removed) {
						while (statementList.remove(next)) {
							// remove duplicates
							duplicateRemoved[0]++;
							logger.trace("removed duplicate of {}", next);
						}
					}
				}
			}
			logger.info("Store scan: total={}, skippedBNode={}, extraInStore={}, duplicateExpectedRemoved={}",
					storeTotal[0], storeSkippedBNode[0], storeExtra[0], duplicateRemoved[0]);
		}
		if (!statementList.isEmpty()) {
			logger.error("Missing {} statements for format {} expected file {}", statementList.size(), format.getName(),
					fileName);
			int sampleLimit = Math.min(5, statementList.size());
			try (SailRepositoryConnection connection = sailRepository.getConnection()) {
				for (int i = 0; i < sampleLimit; i++) {
					Statement missing = statementList.get(i);
					logger.error("Missing sample {}: {}", i + 1, missing);
					try (RepositoryResult<Statement> candidates = connection.getStatements(missing.getSubject(),
							missing.getPredicate(), null, false)) {
						int candidateCount = 0;
						while (candidates.hasNext() && candidateCount < 5) {
							Statement candidate = candidates.next();
							logger.error("Candidate object {}: {}", candidateCount + 1, candidate.getObject());
							candidateCount++;
						}
						if (candidateCount == 0) {
							logger.error("No candidate objects for missing sample {} subject/predicate", i + 1);
						}
					}
				}
			}
			for (Statement statement : statementList) {
				System.err.println(statement);
			}
			Assert.fail(fileName + " contains more triples than the EndpointStore");
		}
	}

	@Test
	public void loadTest() throws IOException {
		logger.info("Starting loadTest for format {} with file {}", format.getName(), fileName);
		sparql.init();
		Sparql.LoadFileResult result = sparql.loadFile(streamOut(fileName), fileName);
		logger.info("loadFile result for format {}: {}", format.getName(), result);
		assertAllCoktailsHDTLoaded();
	}

	@Test
	@Ignore("large test")
	public void loadLargeTest() throws IOException {
		if (format == RDFFormat.HDT)
			return;
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

	private void logFileStats(Path path, String label) {
		try {
			logger.info("File stats ({}): path={}, size={} bytes", label, path.toAbsolutePath(), Files.size(path));
		} catch (IOException e) {
			logger.warn("Unable to stat file ({}): path={}", label, path.toAbsolutePath(), e);
		}
	}
}

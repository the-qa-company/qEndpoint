package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.compiler.CompiledSail;
import com.the_qa_company.qendpoint.compiler.SparqlRepository;
import com.the_qa_company.qendpoint.store.exception.EndpointTimeoutException;
import com.the_qa_company.qendpoint.utils.rdf.ClosableResult;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class EndpointStoreConnectionTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test(expected = EndpointTimeoutException.class)
	public void timeoutTest() throws IOException, ParserException {
		Path tempFile = tempDir.newFolder().toPath();

		Path nativeLocation = tempFile.resolve("native");
		Path hdtLocation = tempFile.resolve("hdt");
		EndpointFiles files = new EndpointFiles(nativeLocation, hdtLocation, "index.hdt");
		Path hdtFile = Path.of(files.getHDTIndex());

		Files.createDirectories(hdtLocation);

		try (HDT hdt = HDTManager.generateHDT(
				List.of(new TripleString(Utility.EXAMPLE_NAMESPACE + "S", Utility.EXAMPLE_NAMESPACE + "P",
						Utility.EXAMPLE_NAMESPACE + "O"),
						new TripleString(Utility.EXAMPLE_NAMESPACE + "S", Utility.EXAMPLE_NAMESPACE + "P",
								Utility.EXAMPLE_NAMESPACE + "O1"),
						new TripleString(Utility.EXAMPLE_NAMESPACE + "S", Utility.EXAMPLE_NAMESPACE + "P",
								Utility.EXAMPLE_NAMESPACE + "O2"))
						.iterator(),
				Utility.EXAMPLE_NAMESPACE, HDTOptions.of(), null)) {
			hdt.saveToHDT(hdtFile.toAbsolutePath().toString(), null);
		}

		SparqlRepository repository;

		try (InputStream modelStream = Objects.requireNonNull(
				getClass().getClassLoader().getResourceAsStream("model/model_example5.ttl"), "Can't read model file")) {
			repository = CompiledSail.compiler().withConfig(modelStream, RDFFormat.TURTLE, false)
					.withEndpointFiles(files).compileToSparqlRepository();
		}
		try {
			EndpointStoreConnection.debugWaittime = 10_000;
			// ?o2
			try (ClosableResult<TupleQueryResult> exec = repository.executeTupleQuery(
					String.join("\n", "PREFIX ex: <" + Utility.EXAMPLE_NAMESPACE + ">", "SELECT * {", "?s ?p ?o", "}"),
					5)) {
				exec.getResult().forEach(System.out::println);
			}
		} catch (SailException | QueryEvaluationException e) {
			throwContainer(e);
		} finally {
			EndpointStoreConnection.debugWaittime = 0;
			repository.shutDown();
		}
	}

	@Test(expected = EndpointTimeoutException.class)
	public void timeoutNoModelTest() throws IOException, ParserException {
		Path tempFile = tempDir.newFolder().toPath();

		Path nativeLocation = tempFile.resolve("native");
		Path hdtLocation = tempFile.resolve("hdt");
		EndpointFiles files = new EndpointFiles(nativeLocation, hdtLocation, "index.hdt");
		Path hdtFile = Path.of(files.getHDTIndex());

		Files.createDirectories(hdtLocation);

		try (HDT hdt = HDTManager.generateHDT(
				List.of(new TripleString(Utility.EXAMPLE_NAMESPACE + "S", Utility.EXAMPLE_NAMESPACE + "P",
						Utility.EXAMPLE_NAMESPACE + "O"),
						new TripleString(Utility.EXAMPLE_NAMESPACE + "S", Utility.EXAMPLE_NAMESPACE + "P",
								Utility.EXAMPLE_NAMESPACE + "O1"),
						new TripleString(Utility.EXAMPLE_NAMESPACE + "S", Utility.EXAMPLE_NAMESPACE + "P",
								Utility.EXAMPLE_NAMESPACE + "O2"))
						.iterator(),
				Utility.EXAMPLE_NAMESPACE, HDTOptions.of(), null)) {
			hdt.saveToHDT(hdtFile.toAbsolutePath().toString(), null);
		}

		SparqlRepository repository = CompiledSail.compiler().withEndpointFiles(files).compileToSparqlRepository();
		try {
			EndpointStoreConnection.debugWaittime = 10_000;
			// ?o2
			try (ClosableResult<TupleQueryResult> exec = repository.executeTupleQuery(
					String.join("\n", "PREFIX ex: <" + Utility.EXAMPLE_NAMESPACE + ">", "SELECT * {", "?s ?p ?o", "}"),
					5)) {
				exec.getResult().forEach(System.out::println);
			}
		} catch (SailException | QueryEvaluationException e) {
			throwContainer(e);
		} finally {
			EndpointStoreConnection.debugWaittime = 0;
			repository.shutDown();
		}
	}

	private static <X extends Throwable> void throwContainer(X e) throws X {
		if (e.getCause() == null) {
			throw e;
		}
		try {
			throw e.getCause();
		} catch (SailException | QueryEvaluationException ee) {
			throwContainer(ee);
		} catch (RuntimeException | Error ee) {
			throw ee;
		} catch (Throwable ee) {
			throw e;
		}
	}
}

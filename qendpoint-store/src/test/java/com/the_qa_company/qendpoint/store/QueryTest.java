package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.compiler.CompiledSail;
import com.the_qa_company.qendpoint.compiler.SparqlRepository;
import com.the_qa_company.qendpoint.utils.rdf.ClosableResult;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class QueryTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();
	private SparqlRepository repository;

	@Before
	public void setupRepo() throws IOException {
		Path root = tempDir.newFolder().toPath();
		ClassLoader loader = getClass().getClassLoader();
		String filename = "issues/issue_94/dataset.nt";

		Path hdtstore = root.resolve("hdt-store");
		Path locationNative = root.resolve("native");

		Files.createDirectories(hdtstore);
		Files.createDirectories(locationNative);

		String indexName = "index.hdt";

		try (HDT hdt = HDTManager.generateHDT(new Iterator<>() {
			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public TripleString next() {
				return null;
			}
		}, Utility.EXAMPLE_NAMESPACE, HDTOptions.of(), null)) {
			hdt.saveToHDT(hdtstore.resolve(indexName).toAbsolutePath().toString(), null);
		} catch (Error | RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		repository = CompiledSail.compiler().withEndpointFiles(new EndpointFiles(locationNative, hdtstore, indexName))
				.compileToSparqlRepository();
		try (InputStream is = Objects.requireNonNull(loader.getResourceAsStream(filename),
				filename + " doesn't exist")) {
			repository.loadFile(is, filename);
		}
	}

	@After
	public void closeRepo() {
		if (repository != null) {
			repository.shutDown();
		}
	}

	private void queryTest(String filename, int outputresults) throws IOException {
		ClassLoader loader = getClass().getClassLoader();
		String query;
		try (InputStream is = Objects.requireNonNull(loader.getResourceAsStream(filename),
				filename + " doesn't exist")) {
			query = IOUtils.toString(is, StandardCharsets.UTF_8);
		}
		try (ClosableResult<TupleQueryResult> result = repository.executeTupleQuery(query, -1)) {
			TupleQueryResult res = result.getResult();
			int c = 0;
			while (res.hasNext()) {
				System.out.println(res.next());
				c++;
			}
			System.out.println("results: " + c);
			assertEquals(outputresults, c);
		}
	}

	@Test
	public void groupByTest1() throws IOException {
		queryTest("issues/issue_94/query1.sparql", 16);
	}

	@Test
	public void groupByTest2() throws IOException {
		queryTest("issues/issue_94/query2.sparql", 5);
	}

}

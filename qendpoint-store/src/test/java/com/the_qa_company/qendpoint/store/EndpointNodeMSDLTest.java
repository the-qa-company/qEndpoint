package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EndpointNodeMSDLTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void epNodeMSDLTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);

		EndpointFiles files = new EndpointFiles(root);

		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(1000, 42)
				.withMaxElementSplit(20).withMaxLiteralSize(30);

		Path hdtPath = files.getHDTIndexPath();
		Files.createDirectories(hdtPath.getParent());
		supplier.createAndSaveFakeHDT(spec, hdtPath);

		EndpointStore store = new EndpointStore(files, spec);
		SailRepository repo = new SailRepository(store);
		repo.init();
		try {
			Repositories.consume(repo, conn -> {
				try {
					long count = 0;
					try (RepositoryResult<Statement> smts = conn.getStatements(null, null, null, false)) {
						while (smts.hasNext()) {
							Statement triple = smts.next();

							String ss = triple.getSubject().toString();
							String ps = triple.getPredicate().toString();
							String os = triple.getObject().toString();

							assertNotNull(ss);
							assertNotNull(ps);
							assertNotNull(os);

							if (!store.getHdt().search(ss, ps, os).hasNext()) {
								fail(format("Can't find triple in store %s %s %s", ss, ps, os));
							}

							count++;
						}
					}
					assertEquals(store.getHdt().getTriples().getNumberOfElements(), count);
					for (TripleString ts : store.getHdt()) {
						assertTrue(ts.isStatic());
						if ((ts.getObject().charAt(0) == '_' && ts.getObject().charAt(1) == ':')
								|| ts.getSubject().charAt(0) == '_' && ts.getSubject().charAt(1) == ':') {
							continue; // can't search for bnode in a query
						}
						String query = "SELECT * {\n%s}\n".formatted(ts.asNtriple().toString());
						try {
							TupleQuery tupleQuery = conn.prepareTupleQuery(query);

							try (TupleQueryResult res = tupleQuery.evaluate()) {
								if (!res.hasNext()) {
									fail("Can't find triple " + ts);
								}
								res.next();

								if (res.hasNext()) {
									do {
										System.out.println(res.next());
									} while (res.hasNext());
									fail("multiple output");
								}
							}

						} catch (Throwable t) {
							System.err.println(query);
							throw t;
						}
					}
				} catch (NotFoundException | IOException e) {
					throw new RuntimeException(e);
				}
			});
		} finally {
			repo.shutDown();
		}
	}
}

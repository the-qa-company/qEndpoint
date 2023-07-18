package com.the_qa_company.qendpoint.store.experimental;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.storage.QEPCore;
import com.the_qa_company.qendpoint.store.Utility;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

public class ExperimentalQEndpointSailTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void sailTest() throws IOException {
		Path root = tempDir.newFolder().toPath();
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);
		ExperimentalQEndpointSail sail = new ExperimentalQEndpointSail(root, spec);
		SailRepository repo = new SailRepository(sail);
		repo.init();
		QEPCore core = sail.getQepCore();
		try {
			System.out.println(core.getDatasets());

			Repositories.consume(repo, conn -> {
				ValueFactory vf = conn.getValueFactory();
				conn.add(vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test2"));
				conn.add(vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test3"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p1"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test3"));
				conn.add(vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test4"));
				conn.add(vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p"), vf.createLiteral("aaa", "fr"));
				conn.add(vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test3"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p1"), vf.createLiteral("123456", XSD.INT));
				conn.add(vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p"), vf.createLiteral("test4"));
			});
			Repositories.consume(repo, conn -> {
				ValueFactory vf = conn.getValueFactory();
				conn.add(vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test2"));
			});
			Repositories.consume(repo, conn -> {
				ValueFactory vf = conn.getValueFactory();
				conn.remove(vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE + "test2"));
			});

			System.out.println(core.getDatasets());

			Repositories.consume(repo, conn -> {
				try (RepositoryResult<Statement> statements = conn.getStatements(null, null, null, false)) {
					statements.forEach(System.out::println);
				}
			});
		} finally {
			repo.shutDown();
		}
	}

}

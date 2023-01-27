package com.the_qa_company.qendpoint.client;

import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.utils.sail.SailTest;
import org.apache.commons.io.file.PathUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import com.the_qa_company.qendpoint.core.options.HDTOptions;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexTest {

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void reloadTest() throws IOException {
		Path epfiles = temp.newFolder("epfiles").toPath();

		IRI s = Values.iri(SailTest.NAMESPACE + "s1");
		IRI p = Values.iri(SailTest.NAMESPACE + "p1");
		IRI o1 = Values.iri(SailTest.NAMESPACE + "o1");
		IRI o2 = Values.iri(SailTest.NAMESPACE + "o2");
		IRI o3 = Values.iri(SailTest.NAMESPACE + "o3");
		Set<Value> o = Set.of(o1, o2, o3);

		EndpointFiles files = new EndpointFiles(epfiles.resolve("native"), epfiles.resolve("hdt"), "index.hdt");
		EndpointStore ep = new EndpointStore(files, HDTOptions.of(), false, true);

		SailRepository repo = new SailRepository(ep);

		try {
			Repositories.consume(repo, connection -> {
				connection.add(s, p, o1);
				connection.add(s, p, o2);
				connection.add(s, p, o3);

				try (RepositoryResult<Statement> statements = connection.getStatements(null, null, null, false)) {
					for (Statement stmt : statements) {
						assertEquals(s, stmt.getSubject());
						assertEquals(p, stmt.getPredicate());
						assertTrue(o.contains(stmt.getObject()));
					}
				}
			});
		} finally {
			repo.shutDown();
		}

		EndpointStore ep2 = new EndpointStore(files, HDTOptions.of(), false, true);

		SailRepository repo2 = new SailRepository(ep2);

		try {
			Repositories.consume(repo2, connection -> {
				try (RepositoryResult<Statement> statements = connection.getStatements(null, null, null, false)) {
					for (Statement stmt : statements) {
						assertEquals(s, stmt.getSubject());
						assertEquals(p, stmt.getPredicate());
						assertTrue(o.contains(stmt.getObject()));
					}
				}
			});
		} finally {
			repo2.shutDown();
		}
	}

	@Test
	public void copyTest() throws IOException {
		Path root = temp.getRoot().toPath();
		Path epfiles = root.resolve("epfiles");
		EndpointFiles files = new EndpointFiles(epfiles.resolve("native"), epfiles.resolve("hdt"), "index.hdt");
		EndpointStore ep = new EndpointStore(files, HDTOptions.of(), false, true);

		SailRepository repo = new SailRepository(ep);

		IRI s = Values.iri(SailTest.NAMESPACE + "s1");
		IRI p = Values.iri(SailTest.NAMESPACE + "p1");
		IRI o1 = Values.iri(SailTest.NAMESPACE + "o1");
		IRI o2 = Values.iri(SailTest.NAMESPACE + "o2");
		IRI o3 = Values.iri(SailTest.NAMESPACE + "o3");
		Set<Value> o = Set.of(o1, o2, o3);

		try {
			Repositories.consume(repo, connection -> {
				connection.add(s, p, o1);
				connection.add(s, p, o2);
				connection.add(s, p, o3);

				try (RepositoryResult<Statement> statements = connection.getStatements(null, null, null, false)) {
					for (Statement stmt : statements) {
						assertEquals(s, stmt.getSubject());
						assertEquals(p, stmt.getPredicate());
						assertTrue(o.contains(stmt.getObject()));
					}
				}
			});
		} finally {
			repo.shutDown();
		}

		Path epfiles2 = root.resolve("epfiles2");

		PathUtils.copyDirectory(epfiles, epfiles2);

		EndpointFiles files2 = new EndpointFiles(epfiles2.resolve("native"), epfiles2.resolve("hdt"), "index.hdt");

		EndpointStore ep2 = new EndpointStore(files2, HDTOptions.of(), false, true);

		SailRepository repo2 = new SailRepository(ep2);

		try {
			Repositories.consume(repo2, connection -> {
				try (RepositoryResult<Statement> statements = connection.getStatements(null, null, null, false)) {
					for (Statement stmt : statements) {
						assertEquals(s, stmt.getSubject());
						assertEquals(p, stmt.getPredicate());
						assertTrue(o.contains(stmt.getObject()));
					}
				}
			});
		} finally {
			repo2.shutDown();
		}
	}
}

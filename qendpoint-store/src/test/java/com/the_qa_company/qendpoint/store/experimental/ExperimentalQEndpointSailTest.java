package com.the_qa_company.qendpoint.store.experimental;

import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.storage.QEPCore;
import com.the_qa_company.qendpoint.core.storage.QEPCoreUtils;
import com.the_qa_company.qendpoint.core.storage.QEPDataset;
import com.the_qa_company.qendpoint.core.storage.iterator.QueryCloseableIterator;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.debug.DebugInjectionPointManager;
import com.the_qa_company.qendpoint.store.Utility;
import com.the_qa_company.qendpoint.utils.RDFStreamUtils;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

	@Test
	@Ignore("large")
	public void largeAddsTest() throws Exception {
		Path root = tempDir.newFolder("generation").toPath();

		final long size = 10_000;
		final int counts = 10;

		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
				.createSupplierWithMaxTriples(size, 4567).withMaxElementSplit(50).withMaxLiteralSize(50);

		LargeFakeDataSetStreamSupplier supplier2 = LargeFakeDataSetStreamSupplier
				.createSupplierWithMaxTriples(size * counts, 4567).withMaxElementSplit(50).withMaxLiteralSize(50);

		Path exceptedHDT = root.resolve("excepted.hdt");
		supplier2.createAndSaveFakeHDT(HDTOptions.empty(), exceptedHDT);

		ExperimentalQEndpointSail sail = new ExperimentalQEndpointSail(root, HDTOptions.of(
		// QEPCore.OPTION_EXECUTOR_THREADS, 1 // force sync
		));
		try (HDT hdt = HDTManager.mapHDT(exceptedHDT)) {

			SailRepository repo = new SailRepository(sail);
			repo.init();

			try {
				StopWatch sw = new StopWatch();

				QEPCoreUtils.getDebugPreBindInsert().registerAction(DebugInjectionPointManager.DebugPolicy.NO_DELETE,
						(qepCore -> {
							System.out.println("preBindTime:  " + sw.stopAndShow());
							sw.reset();
						}));
				QEPCoreUtils.getDebugPostBindInsert().registerAction(DebugInjectionPointManager.DebugPolicy.NO_DELETE,
						(qepCore -> {
							System.out.println("postBindTime: " + sw.stopAndShow());
							sw.reset();
						}));
				for (int i = 1; i <= counts; i++) {
					sw.reset();
					System.out.println("inject");
					Repositories.consume(repo, conn -> supplier.createTripleStringStream().forEachRemaining(
							stmt -> conn.add(RDFStreamUtils.convertStatement(conn.getValueFactory(), stmt))));
					System.out.println(i + " done");
				}

				long sizeGet = Repositories.getNoTransaction(repo, RepositoryConnection::size);

				assertEquals(hdt.getTriples().getNumberOfElements(), sizeGet);
				System.out.println("excepted: " + hdt.getTriples().getNumberOfElements());
				System.out.println("actual: " + sizeGet);
				for (QEPDataset ds : sail.getQepCore().getDatasets()) {
					System.out.println("- " + ds.uid() + " : " + ds.dataset().getTriples().getNumberOfElements());
				}

				System.out.println("Check integrity");

				AtomicLong count = new AtomicLong();
				Repositories.consumeNoTransaction(repo, conn -> {
					try (RepositoryResult<Statement> res = conn.getStatements(null, null, null, false)) {
						while (res.hasNext()) {
							Statement resNext = res.next();
							count.incrementAndGet();

							TripleString ts = new TripleString(resNext.getSubject().toString(),
									resNext.getPredicate().toString(), resNext.getObject().toString());
							try {
								if (!hdt.search(ts).hasNext()) {
									fail("can't find triple: " + resNext + " / " + ts);
								}
							} catch (NotFoundException e) {
								throw new AssertionError(e);
							}
						}
					}

				});
				assertEquals(sizeGet, count.get());
				System.out.println("ok");
			} finally {
				repo.shutDown();
				DebugInjectionPointManager.throwAll(QEPCoreUtils.getDebugPreBindInsert(),
						QEPCoreUtils.getDebugPostBindInsert());
			}
		}
	}
}

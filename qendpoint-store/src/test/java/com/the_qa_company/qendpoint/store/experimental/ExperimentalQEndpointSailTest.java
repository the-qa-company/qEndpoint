package com.the_qa_company.qendpoint.store.experimental;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.storage.QEPCore;
import com.the_qa_company.qendpoint.core.storage.QEPDataset;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.store.Utility;
import com.the_qa_company.qendpoint.utils.RDFStreamUtils;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

public class ExperimentalQEndpointSailTest {
	private static void dumpDatasets(List<QEPDataset> datasets) {
		System.out.println("----------------------- Datasets (" + datasets.size() + ")");
		for (QEPDataset ds : datasets) {
			HDT dataset = ds.dataset();
			String id = ds.id();

			long nsh = dataset.getDictionary().getNshared();
			System.out.printf(
					"ds: [%s] %d (SH:%d S:%d P:%d O:%d)%n",
					id, dataset.getTriples().getNumberOfElements(),
					nsh,
					dataset.getDictionary().getNsubjects() - nsh,
					dataset.getDictionary().getNpredicates(),
					dataset.getDictionary().getNobjects() - nsh);
		}
		System.out.println("-----------------------");
	}
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();


	@Test
	@Ignore("dzqd")
	public void sailTest2() throws IOException {
		Path root = tempDir.newFolder().toPath();
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG,
				HDTOptionsKeys.PROFILER_KEY, true);

		ExperimentalQEndpointSail sail = new ExperimentalQEndpointSail(root, spec);
		SailRepository repo = new SailRepository(sail);
		repo.init();
		QEPCore core = sail.getQepCore();
		try {
			dumpDatasets(core.getDatasets());

			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(50_000, 58)
					.withMaxLiteralSize(50)
					.withMaxElementSplit(300)
					;
			StopWatch sw = new StopWatch();
			StringBuilder csvBld = new StringBuilder("ds,time\n");

			for (int i = 0; i < 10; i++) {
				sw.reset();
				Repositories.consume(repo, conn -> {
					ValueFactory vf = conn.getValueFactory();

					supplier.createTripleStringStream().forEachRemaining(t -> conn.add(RDFStreamUtils.convertStatement(vf, t)));
				});
				System.out.println("#" + i + " Inserted in " + sw.stopAndShow());
				csvBld.append(i).append(",").append(sw.getMeasure() / 1_000_000).append("\n");

				dumpDatasets(core.getDatasets());
			}
			System.out.println("csv:\n" + csvBld);

		} finally {
			repo.shutDown();
		}
	}

	@Test
	@Ignore("dzqd")
	public void sailTestNS2() throws IOException {
		Path root = tempDir.newFolder().toPath();
		NativeStore sail = new NativeStore(root.toFile(), "spoc,posc,cosp");
		SailRepository repo = new SailRepository(sail);
		repo.init();
		try {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(50_000, 58)
					.withMaxLiteralSize(50)
					.withMaxElementSplit(300);
					//.withMaxLiteralSize(1000);
			StopWatch sw = new StopWatch();
			StringBuilder csvBld = new StringBuilder("ds,time\n");
			for (int i = 0; i < 10; i++) {
				sw.reset();
				Repositories.consume(repo, conn -> {
					ValueFactory vf = conn.getValueFactory();

					supplier.createTripleStringStream().forEachRemaining(t -> conn.add(RDFStreamUtils.convertStatement(vf, t)));
				});
				System.out.println("#" + i + " Inserted in " + sw.stopAndShow());
				csvBld.append(i).append(",").append(sw.getMeasure() / 1_000_000).append("\n");
			}

			System.out.println("csv:\n" + csvBld);
		} finally {
			repo.shutDown();
		}
	}

}

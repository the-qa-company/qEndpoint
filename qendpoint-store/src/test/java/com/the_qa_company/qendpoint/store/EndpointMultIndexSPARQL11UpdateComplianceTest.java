package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIndexFile;
import com.the_qa_company.qendpoint.utils.FileUtils;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11UpdateComplianceTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

public class EndpointMultIndexSPARQL11UpdateComplianceTest extends SPARQL11UpdateComplianceTest {

	public EndpointMultIndexSPARQL11UpdateComplianceTest() {
		super();
		List<String> testToIgnore = new ArrayList<>();
		// @todo these tests are failing and should not, they are skipped so
		// that we can be sure that we see when
		// currently passing tests are not failing. Many of these tests are not
		// so problematic since we do not support
		// named graphs anyway
		testToIgnore.add("DELETE INSERT 1b");
		testToIgnore.add("DELETE INSERT 1c");
		testToIgnore.add("CLEAR NAMED");
		testToIgnore.add("DROP NAMED");
		this.setIgnoredTests(testToIgnore);
	}

	@TempDir
	public Path tempDir;

	@Override
	protected Repository newRepository() throws Exception {
		FileUtils.deleteRecursively(tempDir);
		Path nativeStore = tempDir.resolve("ns");
		Path hdtStore = tempDir.resolve("hdt");

		Files.createDirectories(nativeStore);
		Files.createDirectories(hdtStore);

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS,
				EnumSet.of(TripleComponentOrder.SPO, TripleComponentOrder.OPS, TripleComponentOrder.PSO));
		Path fileName = Path.of(hdtStore.toAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME);
		try (HDT hdt = Utility.createTempHdtIndex(hdtStore.resolve("test.nt").toAbsolutePath().toString(), true, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(fileName, null);
		}

		EndpointStore endpoint = new EndpointStore(hdtStore.toAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME,
				spec, nativeStore.toAbsolutePath() + "/", true) {

			@Override
			public HDT loadIndex() throws IOException {
				HDT idx = super.loadIndex();
				if (idx.getTriples().getNumberOfElements() == 0) {
					return idx;
				}
				try {
					Path fileOPS = BitmapTriplesIndexFile.getIndexPath(fileName, TripleComponentOrder.OPS);
					Assertions.assertTrue(Files.exists(fileOPS), "can't find " + fileOPS);
					Path filePSO = BitmapTriplesIndexFile.getIndexPath(fileName, TripleComponentOrder.PSO);
					Assertions.assertTrue(Files.exists(filePSO), "can't find " + filePSO);
				} catch (Throwable t) {
					try (Stream<Path> l = Files.list(fileName.getParent())) {
						l.forEach(System.err::println);
					} catch (Exception e) {
						t.addSuppressed(e);
					}
					throw t;
				}
				return idx;
			}
		};
		// endpoint.setThreshold(2);

		return new SailRepository(endpoint);
		// return new DatasetRepository(new SailRepository(new
		// NativeStore(tempDir.newFolder(), "spoc")));
	}

}

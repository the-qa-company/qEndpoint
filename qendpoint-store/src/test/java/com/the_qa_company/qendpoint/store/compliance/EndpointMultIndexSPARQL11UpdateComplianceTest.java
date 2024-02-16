package com.the_qa_company.qendpoint.store.compliance;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIndexFile;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.EndpointStoreTest;
import com.the_qa_company.qendpoint.store.Utility;
import com.the_qa_company.qendpoint.utils.FileUtils;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.stream.Stream;

public class EndpointMultIndexSPARQL11UpdateComplianceTest extends CTSPARQL11UpdateComplianceTest {
	@TempDir
	public Path tempDirPath;

	public EndpointMultIndexSPARQL11UpdateComplianceTest() {
		super("DELETE INSERT 1b", "DELETE INSERT 1c", "CLEAR NAMED", "DROP NAMED");
	}

	@Override
	protected Sail newSail() throws Exception {
		FileUtils.deleteRecursively(tempDirPath);
		Path nativeStore = tempDirPath.resolve("ns");
		Path hdtStore = tempDirPath.resolve("hdt");

		Files.createDirectories(nativeStore);
		Files.createDirectories(hdtStore);
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS,
				EnumSet.of(TripleComponentOrder.SPO, TripleComponentOrder.OPS, TripleComponentOrder.PSO));
		Path fileName = Path.of(hdtStore.toAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME);
		try (HDT hdt = Utility.createTempHdtIndex(tempDirPath.resolve("test.nt"), true, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(fileName, null);
		}

		return new EndpointStore(hdtStore.toAbsolutePath() + "/",
				EndpointStoreTest.HDT_INDEX_NAME, spec, nativeStore.toAbsolutePath() + "/", true) {

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
	}

}

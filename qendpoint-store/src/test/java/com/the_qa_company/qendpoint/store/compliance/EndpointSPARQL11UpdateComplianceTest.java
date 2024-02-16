package com.the_qa_company.qendpoint.store.compliance;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.EndpointStoreTest;
import com.the_qa_company.qendpoint.store.Utility;
import com.the_qa_company.qendpoint.utils.FileUtils;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

public class EndpointSPARQL11UpdateComplianceTest extends CTSPARQL11UpdateComplianceTest {
	@TempDir
	public Path tempDirPath;

	public EndpointSPARQL11UpdateComplianceTest() {
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
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
		try (HDT hdt = Utility.createTempHdtIndex(tempDirPath.resolve("temp.nt"), true, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(hdtStore.toAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);
		}

		// endpoint.setThreshold(2);

		return new EndpointStore(hdtStore.toAbsolutePath() + "/",
				EndpointStoreTest.HDT_INDEX_NAME, spec, nativeStore.toAbsolutePath() + "/", true);
		// return new DatasetRepository(new SailRepository(new
		// NativeStore(tempDir.newFolder(), "spoc")));
	}

}

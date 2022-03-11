package com.the_qa_company.q_endpoint;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.hybridstore.MergeRunnableStopPoint;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class MergeMethodTest {
	private static final Logger logger = LoggerFactory.getLogger(MergeMethodTest.class);
	private HybridStore store;
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Before
	public void setup() throws IOException, NotFoundException {
		HDTSpecification spec = new HDTSpecification();
		spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
		// set the MergeRunnable in test mode
		MergeRunnableStopPoint.debug = true;
		File root = tempDir.newFolder();
		// create stores dirs
		File nativeStore = new File(root, "native-store");
		nativeStore.mkdirs();
		File hdtStore = new File(root, "hdt-store");
		hdtStore.mkdirs();
		File countFile = new File(root, "count");

		// the number of triples we need
		int count = 4;

		// create a test HDT, saving it and printing it
		HDT hdt = MergeRestartTest.createTestHDT(tempDir.newFile().getAbsolutePath(), spec, count);
		hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + HybridStoreTest.HDT_INDEX_NAME, null);
		MergeRestartTest.printHDT(hdt, null);

		// start a hybrid store
		store = new HybridStore(
				hdtStore.getAbsolutePath() + "/", HybridStoreTest.HDT_INDEX_NAME, spec, nativeStore.getAbsolutePath() + "/", false
		);
	}

	@Test
	public void noThresholdTest() {
		store.mergeStore();
	}

}

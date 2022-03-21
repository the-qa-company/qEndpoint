package com.the_qa_company.q_endpoint.hybridstore;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.junit.Assert;
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
		Assert.assertTrue(nativeStore.mkdirs());
		File hdtStore = new File(root, "hdt-store");
		Assert.assertTrue(hdtStore.mkdirs());

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

	private void addMockElements(int number) {
		ValueFactory vf = store.getValueFactory();
		try (SailConnection connection = store.getConnection()) {
			connection.begin();
			for (int id = 0; id < number; id++) {
				connection.addStatement(
						vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testNat" + id),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE, "id"+id)
				);
			}
			connection.commit();
		}
	}

	@Test
	public void noThresholdTest() throws InterruptedException {
		int elements = 20;
		addMockElements(elements);
		Assert.assertTrue(store.isNativeStoreContainsAtLeast(elements));
		store.mergeStore();
		try {
			store.mergeStore();
			Assert.fail("mergeStore() should crash after a merge");
		} catch (Exception e) {
			// good
		}
		MergeRunnable.debugWaitMerge();
		store.mergeStore();
		Assert.assertFalse(store.isMergeTriggered);
		Assert.assertFalse(store.isNativeStoreContainsAtLeast(1));
	}

}

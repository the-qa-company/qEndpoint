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

import java.io.File;
import java.io.IOException;

public class MergeMethodTest {
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

	private void addMockElements(int number, int idf) {
		ValueFactory vf = store.getValueFactory();
		try (SailConnection connection = store.getConnection()) {
			connection.begin();
			for (int id = 0; id < number; id++) {
				connection.addStatement(
						vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testNat" + id + "-" + idf),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
						vf.createIRI(Utility.EXAMPLE_NAMESPACE, "id"+id + "-" + idf)
				);
			}
			connection.commit();
		}
	}

	@Test
	public void noThresholdTest() throws InterruptedException {
		int elements = 20;
		addMockElements(elements, 0);
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

	@Test
	public void multiMergeTest() throws InterruptedException {
		int elements = 20;
		addMockElements(elements, 1);
		Assert.assertTrue(store.isNativeStoreContainsAtLeast(elements));
		store.mergeStore();
		try {
			store.mergeStore();
			Assert.fail("mergeStore() should crash after a merge");
		} catch (Exception e) {
			// good
		}
		MergeRunnable.debugWaitMerge();
		Assert.assertFalse(store.isNativeStoreContainsAtLeast(1));

		addMockElements(elements, 2);
		Assert.assertTrue(store.isNativeStoreContainsAtLeast(elements));
		store.mergeStore();
		try {
			store.mergeStore();
			Assert.fail("mergeStore() should crash after a merge");
		} catch (Exception e) {
			// good
		}
		MergeRunnable.debugWaitMerge();
		Assert.assertFalse(store.isNativeStoreContainsAtLeast(1));

		addMockElements(elements, 3);
		Assert.assertTrue(store.isNativeStoreContainsAtLeast(elements));
		store.mergeStore();
		try {
			store.mergeStore();
			Assert.fail("mergeStore() should crash after a merge");
		} catch (Exception e) {
			// good
		}
		MergeRunnable.debugWaitMerge();
		Assert.assertFalse(store.isNativeStoreContainsAtLeast(1));
	}

}

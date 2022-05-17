package com.the_qa_company.qendpoint.store;

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
    private EndpointStore store;
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
        hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);
        MergeRestartTest.printHDT(hdt, null);

        // start an endpoint store
        store = new EndpointStore(hdtStore.getAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME, spec,
                nativeStore.getAbsolutePath() + "/", false);

    }

    private void addMockElements(int number, String determinant) {
        ValueFactory vf = store.getValueFactory();
        try (SailConnection connection = store.getConnection()) {
            connection.begin();
            for (int id = 0; id < number; id++) {
                connection.addStatement(vf.createIRI(Utility.EXAMPLE_NAMESPACE, determinant + "-" + id),
                        vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
                        vf.createIRI(Utility.EXAMPLE_NAMESPACE, "id" + id + "-" + determinant));
            }
            connection.commit();
        }
    }

    private boolean sizeLowerThan(long size) {
        return !store.isNativeStoreContainsAtLeast(size);
    }

    private boolean sizeGreaterThan(long size) {
        return store.isNativeStoreContainsAtLeast(size + 1);
    }

    private void assertSizeEquals(long size) {
        Assert.assertFalse("store size lower than " + size, sizeLowerThan(size));
        Assert.assertFalse("store size greater than " + size, sizeGreaterThan(size));
    }

    @Test
    public void noThresholdTest() throws InterruptedException {
        int elements = 20;
        addMockElements(elements, "test");
        assertSizeEquals(elements);
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
        assertSizeEquals(0);
    }

    @Test
    public void multiMergeTest() throws InterruptedException {
        int elements = 20;
        addMockElements(elements, "test1");
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

        addMockElements(elements, "test2");
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

        addMockElements(elements, "test3");
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

    @Test
    public void atLeastMethodTest() throws InterruptedException {
        int elements = 20;

        // test that we have the right count after and before an add (no merging)
        assertSizeEquals(0);
        addMockElements(elements, "test_pre_merge");
        assertSizeEquals(elements); // test_pre_merge * elements

        // stop at the step2_start step
        MergeRunnableStopPoint.STEP2_START.debugLock();
        MergeRunnableStopPoint.STEP2_START.debugLockTest();

        // trigger the store merge
        store.mergeStore();

        MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
        {
            // test that we have the right count after and before an add (while merging)
            assertSizeEquals(0);
            addMockElements(elements, "test_merge");
            assertSizeEquals(elements);
        }
        MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

        // wait for the end of the merge
        MergeRunnable.debugWaitMerge();

        // test that we have the right count after and before an add (after merging)
        assertSizeEquals(elements); // test_merge * elements
        addMockElements(elements, "test_after_merge");
        assertSizeEquals(elements * 2); // test_merge * elements + test_after_merge * elements
    }
}

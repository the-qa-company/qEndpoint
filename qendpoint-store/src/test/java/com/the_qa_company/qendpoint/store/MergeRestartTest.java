package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.compact.bitmap.MultiLayerBitmapWrapper;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.storage.TempBuffOut;
import com.the_qa_company.qendpoint.utils.BitArrayDisk;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class MergeRestartTest {
	@Parameterized.Parameters(name = "quads: {0}")
	public static Collection<Object> params() {
		return List.of(true, false);
	}

	private static final Logger logger = LoggerFactory.getLogger(MergeRestartTest.class);
	private static final File HALT_TEST_DIR = new File("tests", "halt_test_dir");
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();
	HDTOptions spec;
	@Parameterized.Parameter
	public boolean quadTest;

	@Before
	public void setUp() {
		if (quadTest) {
			spec = HDTOptions.of(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY,
					HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH_QUAD, HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD);
		} else {
			spec = HDTOptions.of(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY,
					HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH, HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);
		}
		// set the MergeRunnable in test mode
		MergeRunnableStopPoint.debug = true;
		MergeRunnableStopPoint.unlockAll();
	}

	@After
	public void cleanUp() {
		MergeRunnableStopPoint.unlockAll();
	}

	/**
	 * write inside a file a count
	 *
	 * @param f     the file to write
	 * @param count the count to write
	 * @throws IOException if we can't write into the file
	 * @see #getInfoCount(File)
	 */
	private void writeInfoCount(File f, int count) throws IOException {
		Files.writeString(Paths.get(f.getAbsolutePath()), String.valueOf(count));
	}

	/**
	 * read a count inside a file
	 *
	 * @param f the file to write
	 * @return the count inside the file
	 * @throws IOException if we can't read the file
	 * @see #writeInfoCount(File, int)
	 */
	private int getInfoCount(File f) throws IOException {
		return Integer.parseInt(Files.readString(Paths.get(f.getAbsolutePath())));
	}

	/**
	 * lock a {@link MergeRunnableStopPoint} if it is before the current point
	 *
	 * @param point   the point
	 * @param current the current point
	 * @see #lockIfAfter(MergeRunnableStopPoint, MergeRunnableStopPoint)
	 */
	private void lockIfBefore(MergeRunnableStopPoint point, MergeRunnableStopPoint current) {
		if (current.ordinal() <= point.ordinal()) {
			logger.debug("locking " + point.name().toLowerCase());
			point.debugLockTest();
			point.debugLock();
		} else {
			logger.debug("pass locking " + point.name().toLowerCase());
		}
	}

	/**
	 * lock a {@link MergeRunnableStopPoint} if it is after the current point
	 *
	 * @param point   the point
	 * @param current the current point
	 * @see #lockIfBefore(MergeRunnableStopPoint, MergeRunnableStopPoint)
	 */
	private void lockIfAfter(MergeRunnableStopPoint point, MergeRunnableStopPoint current) {
		if (current.ordinal() >= point.ordinal()) {
			logger.debug("locking " + point.name().toLowerCase());
			point.debugLockTest();
			point.debugLock();
		} else {
			logger.debug("pass locking " + point.name().toLowerCase());
		}
	}

	private void mergeRestartTest1(MergeRunnableStopPoint stopPoint) throws IOException, InterruptedException {
		try (Closer closer = Closer.of()) {
			mergeRestartTest1(stopPoint, MergeRestartTest.HALT_TEST_DIR, closer);
		}
	}

	private void mergeRestartTest2(MergeRunnableStopPoint stopPoint) throws IOException, InterruptedException {
		try (Closer closer = Closer.of()) {
			mergeRestartTest2(stopPoint, MergeRestartTest.HALT_TEST_DIR, closer);
		}
	}

	/**
	 * first stage of the merge test, before the crash
	 *
	 * @param stopPoint the stop point to crash
	 * @param root      the root test directory
	 * @param closer    closer
	 * @throws IOException          io errors
	 * @throws InterruptedException wait errors
	 */
	private void mergeRestartTest1(MergeRunnableStopPoint stopPoint, File root, Closer closer)
			throws IOException, InterruptedException {
		// lock every point we need
		lockIfAfter(MergeRunnableStopPoint.STEP1_START, stopPoint);
		lockIfAfter(MergeRunnableStopPoint.STEP1_TEST_SELECT1, stopPoint);
		lockIfAfter(MergeRunnableStopPoint.STEP1_TEST_SELECT2, stopPoint);
		lockIfAfter(MergeRunnableStopPoint.STEP1_TEST_SELECT3, stopPoint);
		lockIfAfter(MergeRunnableStopPoint.STEP1_TEST_SELECT4, stopPoint);
		lockIfAfter(MergeRunnableStopPoint.STEP2_START, stopPoint);
		lockIfAfter(MergeRunnableStopPoint.STEP2_END, stopPoint);

		// set the current stop point
		MergeRunnable.setStopPoint(stopPoint);

		// create stores dirs
		File nativeStore = new File(root, "native-store");
		Assert.assertTrue("hdtStore directory already exists!", nativeStore.mkdirs());
		File hdtStore = new File(root, "hdt-store");
		Assert.assertTrue("hdtStore directory already exists!", hdtStore.mkdirs());
		File countFile = new File(root, "count");

		// the number of triples we need
		int count = 4;

		// create a test HDT, saving it and printing it
		try (HDT hdt = createTestHDT(tempDir.newFile().getAbsolutePath(), spec, count)) {
			hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);
			logger.info("types: {}/{}", hdt.getDictionary().getType(), hdt.getTriples().getType());
			printHDT(hdt);
		}

		// write the current triples count
		writeInfoCount(countFile, count);

		// start an endpoint store
		EndpointStore store = new EndpointStore(hdtStore.getAbsolutePath() + File.separator,
				EndpointStoreTest.HDT_INDEX_NAME, spec, nativeStore.getAbsolutePath() + File.separator, false);
		logger.debug("--- launching merge with stopPoint=" + stopPoint.name().toLowerCase());

		// set the threshold to control the number of add required to trigger
		// the merge
		store.setThreshold(3);

		// create a sail repository to create connections to the store
		SailRepository endpointStore = new SailRepository(store);
		closer.with((Closeable) endpointStore::shutDown, (Closeable) store::deleteNativeLocks);

		int step = 0;
		try {
			++step;
			executeTestAddRDF(countFile, endpointStore, 1, ++count);
			++step;
			executeTestAddRDF(countFile, endpointStore, 2, ++count);
			++step;
			executeTestAddRDF(countFile, endpointStore, 3, ++count);
			// test that we don't have duplicates
			executeTestAddRDF(countFile, endpointStore, 3, count);
			executeTestAddHDT(countFile, endpointStore, 3, count);
			++step;
			// trigger the merge with this call
			executeTestAddRDF(countFile, endpointStore, 4, ++count);
			++step;

			// try basic actions before the merge step 1 starts
			MergeRunnableStopPoint.STEP1_START.debugWaitForEvent();
			// remove a triple from the RDF
			executeTestRemoveRDF(countFile, endpointStore, 1, --count);
			++step;

			// test if the count is correct
			executeTestCount(countFile, endpointStore, store);
			++step;

			// remove a triple from the HDT
			executeTestRemoveHDT(countFile, endpointStore, 1, --count);
			++step;

			// no duplicate test
			executeTestAddRDF(countFile, endpointStore, 4, count);
			executeTestAddHDT(countFile, endpointStore, 4, count);

			// show the bitmap and the file value
			try (BitArrayDisk arr = new BitArrayDisk(0, hdtStore.getAbsolutePath() + "/triples-delete.arr")) {
				logger.debug("STEP1_TEST_BITMAP0n: {}", arr.printInfo());
			}
			// test if the count is correct
			executeTestCount(countFile, endpointStore, store);
			++step;
			MergeRunnableStopPoint.STEP1_START.debugUnlockTest();

			MergeRunnableStopPoint.STEP1_TEST_SELECT1.debugWaitForEvent();
			// try basic actions during the step 1 at to test select
			if (stopPoint.ordinal() >= MergeRunnableStopPoint.STEP1_TEST_SELECT1.ordinal()) {
				// test if the count is correct
				executeTestCount(countFile, endpointStore, store);
				++step;
			}
			MergeRunnableStopPoint.STEP1_TEST_SELECT1.debugUnlockTest();
			MergeRunnableStopPoint.STEP1_TEST_SELECT2.debugWaitForEvent();
			// try basic actions during the step 1 at to test select
			if (stopPoint.ordinal() >= MergeRunnableStopPoint.STEP1_TEST_SELECT2.ordinal()) {
				// test if the count is correct
				executeTestCount(countFile, endpointStore, store);
				++step;
			}
			MergeRunnableStopPoint.STEP1_TEST_SELECT2.debugUnlockTest();
			MergeRunnableStopPoint.STEP1_TEST_SELECT3.debugWaitForEvent();
			// try basic actions during the step 1 at to test select
			if (stopPoint.ordinal() >= MergeRunnableStopPoint.STEP1_TEST_SELECT3.ordinal()) {
				// test if the count is correct
				executeTestCount(countFile, endpointStore, store);
				++step;
			}
			MergeRunnableStopPoint.STEP1_TEST_SELECT3.debugUnlockTest();
			MergeRunnableStopPoint.STEP1_TEST_SELECT4.debugWaitForEvent();
			// try basic actions during the step 1 at to test select
			if (stopPoint.ordinal() >= MergeRunnableStopPoint.STEP1_TEST_SELECT4.ordinal()) {
				// test if the count is correct
				executeTestCount(countFile, endpointStore, store);
				++step;
			}
			MergeRunnableStopPoint.STEP1_TEST_SELECT4.debugUnlockTest();
			// lock step1
			MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
			// try basic actions before the merge step 2 starts
			if (stopPoint.ordinal() >= MergeRunnableStopPoint.STEP2_START.ordinal()) {
				executeTestRemoveRDF(countFile, endpointStore, 2, --count);
				++step;

				// test if the count is correct
				executeTestCount(countFile, endpointStore, store);
				++step;

				executeTestRemoveHDT(countFile, endpointStore, 2, --count);
				++step;

				// no duplicate test
				executeTestAddRDF(countFile, endpointStore, 4, count);
				executeTestAddHDT(countFile, endpointStore, 4, count);

				// test if the count is correct
				executeTestCount(countFile, endpointStore, store);
				++step;
			}
			MergeRunnableStopPoint.STEP2_START.debugUnlockTest();
			// step 2 stuffs
			// try basic actions before the merge step 2 end
			MergeRunnableStopPoint.STEP2_END.debugWaitForEvent();
			if (stopPoint.ordinal() >= MergeRunnableStopPoint.STEP2_END.ordinal()) {
				executeTestRemoveRDF(countFile, endpointStore, 3, --count);
				++step;

				// test if the count is correct
				executeTestCount(countFile, endpointStore, store);
				++step;

				executeTestRemoveHDT(countFile, endpointStore, 3, --count);
				++step;

				// no duplicate test
				executeTestAddRDF(countFile, endpointStore, 4, count);
				executeTestAddHDT(countFile, endpointStore, 4, count);

				// test if the count is correct
				executeTestCount(countFile, endpointStore, store);
				++step;
			}
			MergeRunnableStopPoint.STEP2_END.debugUnlockTest();
			// step 3 lock is avoiding us to use step3_start and step3_end
			MergeRunnable.debugWaitMerge(); // crash if required
			++step;

			// try basic actions after the end of the last merge step

			// test if the count is correct
			executeTestCount(countFile, endpointStore, store);
			++step;

			// no duplicate test
			executeTestAddRDF(countFile, endpointStore, 4, count);
			executeTestAddHDT(countFile, endpointStore, 4, count);

			// test if the count is correct
			executeTestCount(countFile, endpointStore, store);
			++step;

		} catch (MergeRunnableStopPoint.MergeRunnableException e) {
			e.printStackTrace();
		}
		logger.debug("End step: " + step + ", count: " + count);
	}

	/**
	 * open a connection in the store and return a consumer of it with a value
	 * factory
	 *
	 * @param endpointStore the store to open the connection
	 * @param accept        the consumer
	 * @throws MergeRunnableStopPoint.MergeRunnableStopException if the
	 *                                                           connection was
	 *                                                           stopped by a
	 *                                                           merge crash
	 */
	private void openConnection(SailRepository endpointStore, BiConsumer<ValueFactory, RepositoryConnection> accept)
			throws MergeRunnableStopPoint.MergeRunnableStopException {
		try {
			// open the connection
			try (RepositoryConnection connection = endpointStore.getConnection()) {
				ValueFactory vf = connection.getValueFactory();
				// accept it
				accept.accept(vf, connection);
			}
		} catch (RepositoryException e) {
			Throwable e2 = e;
			// try to find an exception we caused
			while (!(e2 instanceof MergeRunnableStopPoint.MergeRunnableException)) {
				if (e2.getCause() == null) {
					throw e; // it's not ours
				}
				e2 = e2.getCause();
			}
			throw (MergeRunnableStopPoint.MergeRunnableException) e2;
		}
	}

	/**
	 * second stage of the merge test, after the crash
	 *
	 * @param point  the stop point of the crash
	 * @param root   the root test directory
	 * @param closer closer
	 * @throws IOException          io errors
	 * @throws InterruptedException wait errors
	 */
	private void mergeRestartTest2(MergeRunnableStopPoint point, File root, Closer closer)
			throws IOException, InterruptedException {
		// lock merge point we need
		lockIfBefore(MergeRunnableStopPoint.STEP1_TEST_SELECT1, point);
		lockIfBefore(MergeRunnableStopPoint.STEP1_TEST_SELECT2, point);
		lockIfBefore(MergeRunnableStopPoint.STEP1_TEST_SELECT3, point);
		lockIfBefore(MergeRunnableStopPoint.STEP1_TEST_SELECT4, point);
		lockIfBefore(MergeRunnableStopPoint.STEP2_START, point);
		lockIfBefore(MergeRunnableStopPoint.STEP2_END, point);

		// the store files
		File nativeStore = new File(root, "native-store");
		File hdtStore = new File(root, "hdt-store");
		File countFile = new File(root, "count");

		try (BitArrayDisk arr = new BitArrayDisk(4, hdtStore.getAbsolutePath() + "/triples-delete.arr")) {
			logger.debug("test2 restart, count of deleted in hdt: {}", arr.countOnes());
		}

		// lock to get the test count 1 into the 2nd step
		EndpointStore store2 = new EndpointStore(hdtStore.getAbsolutePath() + File.separator,
				EndpointStoreTest.HDT_INDEX_NAME, spec, nativeStore.getAbsolutePath() + File.separator, false);
		SailRepository endpointStore2 = new SailRepository(store2);

		closer.with((Closeable) endpointStore2::shutDown, (Closeable) store2::deleteNativeLocks,
				(Closeable) MergeRunnableStopPoint::unlockAll);
		// a merge should be triggered

		// test at each step if the count is the same
		MergeRunnableStopPoint.STEP1_TEST_SELECT1.debugWaitForEvent();
		if (point.ordinal() <= MergeRunnableStopPoint.STEP1_TEST_SELECT1.ordinal()) {
			// test if the count is correct
			executeTestCount(countFile, endpointStore2, store2);
		}
		MergeRunnableStopPoint.STEP1_TEST_SELECT1.debugUnlockTest();

		MergeRunnableStopPoint.STEP1_TEST_SELECT2.debugWaitForEvent();
		if (point.ordinal() <= MergeRunnableStopPoint.STEP1_TEST_SELECT2.ordinal()) {
			// test if the count is correct
			executeTestCount(countFile, endpointStore2, store2);
		}
		MergeRunnableStopPoint.STEP1_TEST_SELECT2.debugUnlockTest();

		MergeRunnableStopPoint.STEP1_TEST_SELECT3.debugWaitForEvent();
		if (point.ordinal() <= MergeRunnableStopPoint.STEP1_TEST_SELECT3.ordinal()) {
			// test if the count is correct
			executeTestCount(countFile, endpointStore2, store2);
		}
		MergeRunnableStopPoint.STEP1_TEST_SELECT3.debugUnlockTest();

		MergeRunnableStopPoint.STEP1_TEST_SELECT4.debugWaitForEvent();
		if (point.ordinal() <= MergeRunnableStopPoint.STEP1_TEST_SELECT4.ordinal()) {
			// test if the count is correct
			executeTestCount(countFile, endpointStore2, store2);
		}
		MergeRunnableStopPoint.STEP1_TEST_SELECT4.debugUnlockTest();

		MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
		if (point.ordinal() <= MergeRunnableStopPoint.STEP2_START.ordinal()) {
			// test if the count is correct
			executeTestCount(countFile, endpointStore2, store2);
		}
		MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

		MergeRunnableStopPoint.STEP2_END.debugWaitForEvent();
		if (point.ordinal() <= MergeRunnableStopPoint.STEP2_END.ordinal()) {
			// test if the count is correct
			executeTestCount(countFile, endpointStore2, store2);
		}
		MergeRunnableStopPoint.STEP2_END.debugUnlockTest();

		// wait for the merge to complete
		MergeRunnable.debugWaitMerge();

		// test if the count is correct
		executeTestCount(countFile, endpointStore2, store2);
	}

	public void mergeRestartTest(MergeRunnableStopPoint stopPoint) throws IOException, InterruptedException {
		MergeRunnableStopPoint.disableRequest = false;
		MergeRunnableStopPoint.unlockAll();
		File testRoot = tempDir.newFolder();
		File root1 = new File(testRoot, "root1");
		File root2 = new File(testRoot, "root2");
		try (Closer closer = Closer.of()) {
			// create a store to tell which dir we are using

			// start the first phase
			mergeRestartTest1(stopPoint, root1, closer);
			// re-allow the request, this was set to true in the first phase
			// crash
			MergeRunnableStopPoint.disableRequest = false;
			// MergeRunnableStopPoint.unlockAllLocks();

			// switch the directory we are using
			swapDir(root1, root2);

			// start the second phase
			mergeRestartTest2(stopPoint, root2, closer);
		} catch (Throwable t) {
			try {
				FileUtils.deleteDirectory(testRoot);
			} catch (IOException e) {
				t.addSuppressed(e);
			}
			throw t;
		} finally {
			MergeRunnableStopPoint.disableRequest = false;
		}
		FileUtils.deleteDirectory(testRoot);
	}

	/**
	 * prepare a halt test
	 */
	public void startHalt() throws IOException {
		// ask for the usage of halt(int) instead of throw to crash the merge
		// process
		MergeRunnableStopPoint.askCompleteFailure();
		// delete previous test data, we can't use tempDir because the value is
		// updated every test restart
		deleteDir(HALT_TEST_DIR);
		// assert we have recreated the test directory deleted in the previous
		// line
		Assert.assertTrue(HALT_TEST_DIR.mkdirs());
	}

	/**
	 * end a halt test
	 */
	public void endHalt() throws IOException {
		// delete the test data
		deleteDir(HALT_TEST_DIR);
		// assert we have deleted the test data
		Assert.assertFalse(HALT_TEST_DIR.exists());
	}

	/* test with throw/wait */
	@Test
	public void mergeRestartStep1StartTest() throws IOException, InterruptedException {
		mergeRestartTest(MergeRunnableStopPoint.STEP1_START);
	}

	@Test
	public void mergeRestartStep1EndTest() throws IOException, InterruptedException {
		mergeRestartTest(MergeRunnableStopPoint.STEP1_END);
	}

	@Test
	public void mergeRestartStep2StartTest() throws IOException, InterruptedException {
		mergeRestartTest(MergeRunnableStopPoint.STEP2_START);
	}

	@Test
	public void mergeRestartStep2EndTest() throws IOException, InterruptedException {
		mergeRestartTest(MergeRunnableStopPoint.STEP2_END);
	}

	@Test
	public void mergeRestartStep3StartTest() throws IOException, InterruptedException {
		mergeRestartTest(MergeRunnableStopPoint.STEP3_START);
	}

	@Test
	public void mergeRestartStep3Mid1Test() throws IOException, InterruptedException {
		mergeRestartTest(MergeRunnableStopPoint.STEP3_FILES_MID1);
	}

	@Test
	public void mergeRestartStep3Mid2Test() throws IOException, InterruptedException {
		mergeRestartTest(MergeRunnableStopPoint.STEP3_FILES_MID2);
	}

	@Test
	public void mergeRestartStep3EndTest() throws IOException, InterruptedException {
		mergeRestartTest(MergeRunnableStopPoint.STEP3_END);
	}

	@Test
	public void mergeRestartMergeEndTest() throws IOException, InterruptedException {
		mergeRestartTest(MergeRunnableStopPoint.MERGE_END);
	}

	@Test
	public void mergeRestartMergeEndAfterSleepTest() throws IOException, InterruptedException {
		mergeRestartTest(MergeRunnableStopPoint.MERGE_END_OLD_SLEEP);
	}

	/* test with throw/wait */
	@Test
	@Ignore("should be used by hand | halt test")
	public void haltMergeRestartStep1StartTest() throws IOException, InterruptedException {
		startHalt();
		mergeRestartTest1(MergeRunnableStopPoint.STEP1_START);
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void halt2MergeRestartStep1StartTest() throws IOException, InterruptedException {
		mergeRestartTest2(MergeRunnableStopPoint.STEP1_START);
		endHalt();
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void haltMergeRestartStep1EndTest() throws IOException, InterruptedException {
		startHalt();
		mergeRestartTest1(MergeRunnableStopPoint.STEP1_END);
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void halt2MergeRestartStep1EndTest() throws IOException, InterruptedException {
		mergeRestartTest2(MergeRunnableStopPoint.STEP1_END);
		endHalt();
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void haltMergeRestartStep2StartTest() throws IOException, InterruptedException {
		startHalt();
		mergeRestartTest1(MergeRunnableStopPoint.STEP2_START);
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void halt2MergeRestartStep2StartTest() throws IOException, InterruptedException {
		mergeRestartTest2(MergeRunnableStopPoint.STEP2_START);
		endHalt();
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void haltMergeRestartStep2EndTest() throws IOException, InterruptedException {
		startHalt();
		mergeRestartTest1(MergeRunnableStopPoint.STEP2_END);
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void halt2MergeRestartStep2EndTest() throws IOException, InterruptedException {
		mergeRestartTest2(MergeRunnableStopPoint.STEP2_END);
		endHalt();
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void haltMergeRestartStep3StartTest() throws IOException, InterruptedException {
		startHalt();
		mergeRestartTest1(MergeRunnableStopPoint.STEP3_START);
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void halt2MergeRestartStep3StartTest() throws IOException, InterruptedException {
		mergeRestartTest2(MergeRunnableStopPoint.STEP3_START);
		endHalt();
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void haltMergeRestartStep3Mid1Test() throws IOException, InterruptedException {
		startHalt();
		mergeRestartTest1(MergeRunnableStopPoint.STEP3_FILES_MID1);
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void halt2MergeRestartStep3Mid1Test() throws IOException, InterruptedException {
		mergeRestartTest2(MergeRunnableStopPoint.STEP3_FILES_MID1);
		endHalt();
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void haltMergeRestartStep3Mid2Test() throws IOException, InterruptedException {
		startHalt();
		mergeRestartTest1(MergeRunnableStopPoint.STEP3_FILES_MID2);
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void halt2MergeRestartStep3Mid2Test() throws IOException, InterruptedException {
		mergeRestartTest2(MergeRunnableStopPoint.STEP3_FILES_MID2);
		endHalt();
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void haltMergeRestartStep3EndTest() throws IOException, InterruptedException {
		startHalt();
		mergeRestartTest1(MergeRunnableStopPoint.STEP3_END);
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void halt2MergeRestartStep3EndTest() throws IOException, InterruptedException {
		mergeRestartTest2(MergeRunnableStopPoint.STEP3_END);
		endHalt();
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void haltMergeRestartMergeEndTest() throws IOException, InterruptedException {
		startHalt();
		mergeRestartTest1(MergeRunnableStopPoint.MERGE_END);
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void halt2MergeRestartMergeEndTest() throws IOException, InterruptedException {
		mergeRestartTest2(MergeRunnableStopPoint.MERGE_END);
		endHalt();
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void haltMergeRestartMergeEndAfterSleepTest() throws IOException, InterruptedException {
		startHalt();
		mergeRestartTest1(MergeRunnableStopPoint.MERGE_END_OLD_SLEEP);
	}

	@Test
	@Ignore("should be used by hand | halt test")
	public void halt2MergeRestartMergeEndAfterSleepTest() throws IOException, InterruptedException {
		mergeRestartTest2(MergeRunnableStopPoint.MERGE_END_OLD_SLEEP);
		endHalt();
	}

	/**
	 * print the HDT and the bitmap (if store not null)
	 *
	 * @param hdt the hdt to print
	 */
	public static void printHDT(HDT hdt) {
		IteratorTripleString it;
		try {
			it = hdt.search("", "", "");
		} catch (NotFoundException e) {
			throw new AssertionError(e);
		}
		logger.debug("HDT: ");
		while (it.hasNext()) {
			logger.debug("- {}", it.next());
		}
	}

	/**
	 * recursively delete a directory
	 *
	 * @param f the directory to delete
	 */
	private void deleteDir(File f) throws IOException {
		Files.walkFileTree(Paths.get(f.getAbsolutePath()), new FileVisitor<>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) {
				return FileVisitResult.TERMINATE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * move all the files from tempDir to tempDir2 and delete tempDir2
	 */
	private void swapDir(File root1, File root2) throws IOException {
		Path to = root2.toPath();
		Path root = root1.toPath();
		Files.walkFileTree(root, new FileVisitor<>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				Path newPath = to.resolve(root.relativize(dir));
				Files.createDirectories(newPath);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Path newFile = to.resolve(root.relativize(file));
				Files.copy(file, newFile);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) {
				return FileVisitResult.TERMINATE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * print and count all the triples of a connection
	 *
	 * @param connection the connection to read
	 * @return the count of triples
	 */
	private int count(RepositoryConnection connection) {
		logger.debug("-- list");
		RepositoryResult<Statement> sts = connection.getStatements(null, null, null, true);
		int count = 0;
		while (sts.hasNext()) {
			logger.debug(String.valueOf(sts.next()));
			count++;
		}
		return count;
	}

	/**
	 * create a test HDT of size n
	 *
	 * @param fileName     the hdt file
	 * @param spec         the hdt spec
	 * @param testElements n
	 * @return the hdt
	 */
	public static HDT createTestHDT(String fileName, HDTOptions spec, int testElements) {
		try {
			File inputFile = new File(fileName);
			String baseURI = inputFile.toURI().toString();
			// adding triples

			ValueFactory vf = new MemValueFactory();
			try (FileOutputStream out = new FileOutputStream(inputFile)) {
				RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
				writer.startRDF();
				logger.debug("Initial HDT:");
				for (int id = 1; id <= testElements; id++) {
					Statement stm = vf.createStatement(vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testHDT" + id),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE, "Bidule"));
					logger.debug("HDT statement: " + stm);
					writer.handleStatement(stm);
				}
				writer.endRDF();
			}

			HDT hdt = HDTManager.generateHDT(inputFile.getAbsolutePath(), baseURI, RDFNotation.NTRIPLES, spec, null);
			return HDTManager.indexedHDT(hdt, null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * remove a triple from the HDT and write the count
	 *
	 * @param out   the count file
	 * @param repo  the store to connect
	 * @param id    the id of the triple
	 * @param count the count with this action
	 * @throws IOException file write error
	 */
	private void executeTestRemoveHDT(File out, SailRepository repo, int id, int count) throws IOException {
		openConnection(repo, (vf, connection) -> {
			Statement stm = vf.createStatement(vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testHDT" + id),
					vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
					vf.createIRI(Utility.EXAMPLE_NAMESPACE, "Bidule"));
			logger.debug("Remove statement " + stm);
			connection.remove(stm);
		});
		writeInfoCount(out, count);
		try (OutputStream buff = new TempBuffOut(new FileOutputStream(out.getAbsolutePath() + ".delta", true))) {
			buff.write(("REM HDT " + id + " / " + count + "\n").getBytes(StandardCharsets.UTF_8));
		}

	}

	/**
	 * remove a triple from the RDF and write the count
	 *
	 * @param out   the count file
	 * @param repo  the store to connect
	 * @param id    the id of the triple
	 * @param count the count with this action
	 * @throws IOException file write error
	 */
	private void executeTestRemoveRDF(File out, SailRepository repo, int id, int count) throws IOException {
		openConnection(repo, (vf, connection) -> {
			Statement stm = vf.createStatement(vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testRDF" + id),
					vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
					vf.createIRI(Utility.EXAMPLE_NAMESPACE, "Bidule"));
			logger.debug("Remove statement " + stm);
			connection.remove(stm);
		});
		writeInfoCount(out, count);
		try (OutputStream buff = new TempBuffOut(new FileOutputStream(out.getAbsolutePath() + ".delta", true))) {
			buff.write(("REM RDF " + id + " / " + count + "\n").getBytes(StandardCharsets.UTF_8));
		}
	}

	/**
	 * add a triple to the native store and write the count
	 *
	 * @param out   the count file
	 * @param repo  the store to connect
	 * @param id    the id of the triple
	 * @param count the count with this action
	 * @throws IOException file write error
	 */
	private void executeTestAddRDF(File out, SailRepository repo, int id, int count) throws IOException {
		openConnection(repo, (vf, connection) -> {
			Statement stm = vf.createStatement(vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testRDF" + id),
					vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
					vf.createIRI(Utility.EXAMPLE_NAMESPACE, "Bidule"));
			logger.debug("Add statement " + stm);
			connection.add(stm);
		});
		writeInfoCount(out, count);
		try (OutputStream buff = new TempBuffOut(new FileOutputStream(out.getAbsolutePath() + ".delta", true))) {
			buff.write(("ADD RDF " + id + " / " + count + "\n").getBytes(StandardCharsets.UTF_8));
		}
	}

	/**
	 * add a triple of the HDT to the native store to test duplicates and write
	 * the count
	 *
	 * @param out   the count file
	 * @param repo  the store to connect
	 * @param id    the id of the triple
	 * @param count the count with this action
	 * @throws IOException file write error
	 */
	private void executeTestAddHDT(File out, SailRepository repo, int id, int count) throws IOException {
		openConnection(repo, (vf, connection) -> {
			Statement stm = vf.createStatement(vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testHDT" + id),
					vf.createIRI(Utility.EXAMPLE_NAMESPACE, "testP"),
					vf.createIRI(Utility.EXAMPLE_NAMESPACE, "Bidule"));
			logger.debug("Add statement " + stm);
			connection.add(stm);
		});
		writeInfoCount(out, count);
		try (OutputStream buff = new TempBuffOut(new FileOutputStream(out.getAbsolutePath() + ".delta", true))) {
			buff.write(("ADD HDT " + id + " / " + count + "\n").getBytes(StandardCharsets.UTF_8));
		}
	}

	/**
	 * count the number of triples and compare it with the count file value
	 *
	 * @param out   the count file
	 * @param repo  the repo to connect
	 * @param store the store to print the HDT
	 * @throws IOException file read error
	 */
	private void executeTestCount(File out, SailRepository repo, EndpointStore store) throws IOException {
		int excepted = getInfoCount(out);
		openConnection(repo, (vf, connection) -> {
			int count = count(connection);
			System.out.println("values:");

			if (count != excepted) {
				try (RepositoryResult<Statement> query = connection.getStatements(null, null, null, false)) {
					query.forEach(EndpointStoreGraphTest::printStmt);
				}
				if (store != null) {
					System.out.println("curr ns: " + (store.switchStore ? "2" : "1"));
					System.out.println("ns1:");
					Consumer<Statement> printer = stmt -> {
						HDTConverter converter = store.getHdtConverter();
						EndpointStoreGraphTest.printStmt(converter.rdf4ToHdt(stmt));
					};
					try (NotifyingSailConnection conn = store.getNativeStoreA().getConnection()) {
						try (CloseableIteration<? extends Statement> it = conn.getStatements(null, null, null, false)) {
							it.forEachRemaining(printer);
						}
					}
					System.out.println("ns2:");
					try (NotifyingSailConnection conn = store.getNativeStoreB().getConnection()) {
						try (CloseableIteration<? extends Statement> it = conn.getStatements(null, null, null, false)) {
							it.forEachRemaining(printer);
						}
					}
					System.out.println("hdt:");
					try {
						store.getHdt().searchAll().forEachRemaining(System.out::println);
					} catch (NotFoundException e) {
						throw new RuntimeException(e);
					}
					System.out.println("bitmaps:");

					long size = store.getHdt().getTriples().getNumberOfElements();
					long graphs = store.getGraphsCount();
					for (int i = 0; i < store.getDeleteBitMaps().length; i++) {
						TripleComponentOrder order = TripleComponentOrder.values()[i];
						MultiLayerBitmapWrapper.MultiLayerModBitmapWrapper bm = store.getDeleteBitMaps()[i];
						if (bm == null) {
							continue;
						}

						System.out.println(order);
						for (int g = 0; g < graphs; g++) {
							System.out.print("g" + (g + 1) + ": ");
							for (long t = 0; t < size; t++) {
								System.out.print(bm.access(g, t) ? "1" : "0");
							}
							System.out.println();
						}
					}
				}

				System.out.println("operations:");
				try {
					System.out.println(Files.readString(Path.of(out.getAbsolutePath() + ".delta")));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				fail(format("count:%d != excepted:%d : Invalid test count", count, excepted));
			}
		});
	}
}

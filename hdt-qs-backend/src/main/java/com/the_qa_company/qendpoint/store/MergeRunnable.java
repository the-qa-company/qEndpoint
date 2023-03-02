package com.the_qa_company.qendpoint.store;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import com.the_qa_company.qendpoint.controller.Sparql;
import com.the_qa_company.qendpoint.store.exception.EndpointStoreException;
import com.the_qa_company.qendpoint.utils.BitArrayDisk;
import com.the_qa_company.qendpoint.utils.OverrideHDTOptions;
import org.apache.commons.io.file.PathUtils;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.concurrent.locks.LockManager;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import com.the_qa_company.qendpoint.core.compact.bitmap.BitmapFactory;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;

public class MergeRunnable {
	/**
	 * class to use {@link MergeThread}
	 *
	 * @param <T> the merge thread type
	 */
	@FunctionalInterface
	private interface MergeThreadRunnable<T> {
		/**
		 * execute a merge step and the next step
		 *
		 * @param restarting if the step is restarting
		 * @param data       if restarting == true, the return value of
		 *                   {@link MergeThreadReloader#reload()}
		 * @throws InterruptedException in case of interruption
		 * @throws IOException          in case of IO error
		 */
		void run(boolean restarting, T data) throws InterruptedException, IOException;
	}

	/**
	 * class to use {@link MergeThread}
	 *
	 * @param <T> the merge thread type
	 */
	@FunctionalInterface
	private interface MergeThreadReloader<T> {
		/**
		 * reload the previous merge data
		 *
		 * @return a lock if required by the step
		 */
		T reload() throws Exception;
	}

	private static final int MERGE_OLD_TO_NEW_SPLIT = 10_000;

	/////// this part is for testing purposes ///////
	// it extends the merging process to this amount of seconds. If -1 then it
	/////// is not set.
	private static int extendsTimeMergeBeginning = -1;
	private static int extendsTimeMergeBeginningAfterSwitch = -1;
	private static int extendsTimeMergeEnd = -1;
	// If stopPoint != null, it will throw a
	// MergeRunnableStopPoint#MergeRunnableStopException
	private static MergeRunnableStopPoint stopPoint;
	// store last merge exception
	private static final LockManager MERGE_THREAD_LOCK_MANAGER = new LockManager();
	// the last exception during the merge
	private static Exception debugLastMergeException;
	/////////////////////////////////////////////////

	public static int getExtendsTimeMergeBeginning() {
		return extendsTimeMergeBeginning;
	}

	public static int getExtendsTimeMergeBeginningAfterSwitch() {
		return extendsTimeMergeBeginningAfterSwitch;
	}

	public static int getExtendsTimeMergeEnd() {
		return extendsTimeMergeEnd;
	}

	public static void setExtendsTimeMergeBeginning(int extendsTimeMergeBeginning) {
		MergeRunnable.extendsTimeMergeBeginning = extendsTimeMergeBeginning;
	}

	public static void setExtendsTimeMergeBeginningAfterSwitch(int extendsTimeMergeBeginningAfterSwitch) {
		MergeRunnable.extendsTimeMergeBeginningAfterSwitch = extendsTimeMergeBeginningAfterSwitch;
	}

	public static void setExtendsTimeMergeEnd(int extendsTimeMergeEnd) {
		MergeRunnable.extendsTimeMergeEnd = extendsTimeMergeEnd;
	}

	/**
	 * wait for the merge to complete, if the merge was stopped because of an
	 * exception (except with {@link #setStopPoint(MergeRunnableStopPoint)}
	 * exception), it is thrown inside a RuntimeException
	 *
	 * @throws InterruptedException in case of interruption
	 */
	public static void debugWaitMerge() throws InterruptedException {
		MERGE_THREAD_LOCK_MANAGER.waitForActiveLocks();
		if (debugLastMergeException != null) {
			if (!(debugLastMergeException instanceof MergeRunnableStopPoint.MergeRunnableStopException)) {
				Exception old = debugLastMergeException;
				debugLastMergeException = null;
				throw new RuntimeException(old);
			}
		}
	}

	/**
	 * set the next stop point in a merge, it would be used only once
	 *
	 * @param stopPoint the stop point
	 */
	public static void setStopPoint(MergeRunnableStopPoint stopPoint) {
		MergeRunnable.stopPoint = stopPoint;
	}

	/**
	 * try delete a file
	 *
	 * @param file the file to delete
	 */
	private static void delete(String file) {
		try {
			Files.delete(Path.of(file));
		} catch (IOException e) {
			logger.warn("Can't delete the file {} ({})", file, e.getClass().getName());
			if (MergeRunnableStopPoint.debug)
				throw new RuntimeException(e);
		}
	}

	/**
	 * try delete a file if it exists
	 *
	 * @param file the file to delete
	 */
	private static void deleteIfExists(String file) {
		try {
			Files.deleteIfExists(Path.of(file));
		} catch (IOException e) {
			logger.warn("Can't delete the file {} ({})", file, e.getClass().getName());
			if (MergeRunnableStopPoint.debug)
				throw new RuntimeException(e);
		}
	}

	// .old file extension
	private static final String OLD_EXT = ".old";

	/**
	 * delete "file + {@link #OLD_EXT}"
	 *
	 * @param file file
	 */
	private static void deleteOld(String file) {
		delete(file + OLD_EXT);
	}

	/**
	 * test if the file exists
	 *
	 * @param file the file
	 * @return true if the file exists, false otherwise
	 */
	private static boolean exists(String file) {
		return Files.exists(Path.of(file));
	}

	/**
	 * test if the file "file + {@link #OLD_EXT}" exists
	 *
	 * @param file the file
	 * @return true if the file exists, false otherwise
	 */
	private static boolean existsOld(String file) {
		return exists(file + OLD_EXT);
	}

	/**
	 * rename file to "file + {@link #OLD_EXT}"
	 *
	 * @param file file
	 */
	private static void renameToOld(String file) {
		rename(file, file + OLD_EXT);
	}

	/**
	 * rename "file + {@link #OLD_EXT}" to file
	 *
	 * @param file file
	 */
	private static void renameFromOld(String file) {
		rename(file + OLD_EXT, file);
	}

	/**
	 * rename a file to another
	 *
	 * @param oldFile the current name
	 * @param newFile the new name
	 */
	private static void rename(String oldFile, String newFile) {
		try {
			Files.move(Path.of(oldFile), Path.of(newFile), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			logger.warn("Can't rename the file {} into {} ({})", oldFile, newFile, e.getClass().getName());
			if (MergeRunnableStopPoint.debug)
				throw new RuntimeException(e);
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(MergeRunnable.class);

	public class MergeThread<T> extends Thread {
		// actions of the merge
		private final MergeThreadRunnable<T> exceptionRunnable;
		private MergeThreadReloader<T> reloadData;
		private Runnable preload;
		// the data returned by the reloadData reloader
		private T data;
		// if the merge is a restarting one
		private final boolean restart;
		// lock for MergeRunnableStopPoint#debugWaitForEvent()
		private Lock debugLock;

		private MergeThread(MergeThreadRunnable<T> run, MergeThreadReloader<T> reloadData) {
			this(null, run, reloadData);
		}

		private MergeThread(Runnable preload, MergeThreadRunnable<T> run, MergeThreadReloader<T> reloadData) {
			this(run, true);
			this.reloadData = reloadData;
			this.preload = preload;
		}

		private MergeThread(MergeThreadRunnable<T> run, boolean restart) {
			super("MergeThread");
			this.exceptionRunnable = run;
			this.restart = restart;
			setDaemon(true);
		}

		/**
		 * preload the data of the merge
		 */
		public void preLoad() {
			if (preload != null) {
				preload.run();
			}
		}

		@Override
		public void run() {
			try {
				this.exceptionRunnable.run(restart, data);
			} catch (IOException e) {
				synchronized (MergeRunnable.this) {
					endpoint.setMerging(false);
				}
				if (MergeRunnableStopPoint.debug)
					debugLastMergeException = e;
				e.printStackTrace();
			} catch (Exception e) {
				if (MergeRunnableStopPoint.debug)
					debugLastMergeException = e;
				e.printStackTrace();
			} finally {
				if (MergeRunnableStopPoint.debug)
					debugLock.release();
			}
		}

		@Override
		public synchronized void start() {
			if (MergeRunnableStopPoint.debug) {
				// create a lock to use the method
				// MergeRunnableStopPoint#debugWaitForEvent()
				if (debugLastMergeException != null
						&& !(debugLastMergeException instanceof MergeRunnableStopPoint.MergeRunnableStopException)) {
					Exception old = debugLastMergeException;
					debugLastMergeException = null;
					throw new RuntimeException("old exception not triggered", old);
				}
				debugLock = MERGE_THREAD_LOCK_MANAGER.createLock("thread");
			}
			if (reloadData != null) {
				// reload the data if required
				try {
					data = reloadData.reload();
				} catch (Exception e) {
					throw new EndpointStoreException("Can't restart merge", e);
				}
			}
			// start the thread
			super.start();
		}
	}

	// the store
	private final EndpointStore endpoint;
	// the files to use
	private final EndpointFiles endpointFiles;

	/**
	 * create a merge runnable handler
	 *
	 * @param endpoint the store to handle
	 */
	public MergeRunnable(EndpointStore endpoint) {
		this.endpoint = endpoint;
		this.endpointFiles = endpoint.getEndpointFiles();
	}

	/**
	 * create a lock to prevent new connection
	 *
	 * @return the {@link Lock}
	 */
	private Lock createConnectionLock() {
		Lock l = endpoint.lockToPreventNewConnections.createLock("translate-lock");

		if (MergeRunnableStopPoint.debug) {
			MergeRunnableStopPoint.setLastLock(l);
		}

		return l;
	}

	/**
	 * create a lock to prevent new update
	 *
	 * @return the {@link Lock}
	 */
	private Lock createUpdateLock() {
		Lock l = endpoint.lockToPreventNewUpdate.createLock("step1-lock");

		if (MergeRunnableStopPoint.debug) {
			MergeRunnableStopPoint.setLastLock(l);
		}

		return l;
	}

	/**
	 * only for test purpose, crash if the stopPoint set with
	 * {@link #setStopPoint(MergeRunnableStopPoint)} == point
	 *
	 * @param point the point to crash
	 * @throws MergeRunnableStopPoint.MergeRunnableStopException if this point
	 *                                                           is selected
	 */
	private void debugStepPoint(MergeRunnableStopPoint point) {
		if (!MergeRunnableStopPoint.debug)
			return;

		point.debugUnlock();
		point.debugWaitForTestEvent();

		logger.debug("Complete merge runnable step " + point.name());
		if (stopPoint == point) {
			stopPoint = null;
			point.debugThrowStop();
		}

	}

	/**
	 * wait all active connection locks
	 *
	 * @throws InterruptedException in case of interruption
	 */
	private void waitForActiveConnections() throws InterruptedException {
		logger.info("Waiting for connections...");
		endpoint.locksHoldByConnections.waitForActiveLocks();
		logger.info("All connections completed.");
	}

	/**
	 * wait all active updates locks
	 *
	 * @throws InterruptedException in case of interruption
	 */
	private void waitForActiveUpdates() throws InterruptedException {
		logger.info("Waiting for updates...");
		endpoint.locksHoldByUpdates.waitForActiveLocks();
		logger.info("All updates completed.");
	}

	/**
	 * @return a new thread to merge the store
	 */
	public MergeThread<?> createThread() {
		return new MergeThread<>(this::step1, false);
	}

	/**
	 * @return an optional thread to restart a previous merge (if any)
	 */
	public Optional<MergeThread<?>> createRestartThread() {
		// @todo: check previous step with into previousMergeFile, return a
		// thread with the runStep
		int step = getRestartStep();
		logger.debug("Restart step: {}", step);
		return switch (step) {
		case 0 -> Optional.of(new MergeThread<>(this::step1, this::reloadDataFromStep1));
		case 2 -> Optional.of(new MergeThread<>(this::step2, this::reloadDataFromStep2));
		case 3 -> Optional.of(new MergeThread<>(this::preloadStep3, this::step3, this::reloadDataFromStep3));
		default -> Optional.empty();
		};
	}

	/**
	 * write the restart step in the merge file
	 *
	 * @param step the restart step to write
	 * @throws IOException see
	 *                     {@link Files#writeString(Path, CharSequence, OpenOption...)}
	 *                     ioe
	 */
	private void markRestartStepCompleted(int step) throws IOException {
		Files.writeString(Path.of(endpointFiles.getPreviousMergeFile()), String.valueOf(step));
	}

	/**
	 * @return the restart step in the merge file, -1 in case of error
	 */
	private int getRestartStep() {
		try {
			String text = Files.readString(Path.of(endpointFiles.getPreviousMergeFile()));
			return Integer.parseInt(text.trim());
		} catch (IOException | NumberFormatException e) {
			return -1;
		}
	}

	/**
	 * delete the merge file
	 *
	 * @throws IOException see {@link Files#delete(Path)} ioe
	 */
	private void completedMerge() throws IOException {
		Files.delete(Path.of(endpointFiles.getPreviousMergeFile()));
	}

	/**
	 * (debug), sleep for seconds s if seconds != -1
	 *
	 * @param seconds the number of seconds to sleep
	 * @param title   the title in the debug logs
	 */
	private void sleep(int seconds, String title) {
		if (seconds != -1) {
			try {
				logger.debug("It is sleeping " + title + " " + seconds);
				Thread.sleep(seconds * 1000L);
				logger.debug("Finished sleeping " + title);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * reload previous data from step 1
	 *
	 * @return previous data from step 2
	 */
	private Lock reloadDataFromStep1() {
		return createUpdateLock();
	}

	/**
	 * start the merge at step1
	 *
	 * @param restarting if we are restarting from step 1 or not
	 * @param switchLock the return value or {@link #reloadDataFromStep1()}
	 * @throws InterruptedException for wait exception
	 * @throws IOException          for file exception
	 */
	private synchronized void step1(boolean restarting, Lock switchLock) throws InterruptedException, IOException {
		logger.info("Start Merge process...");
		markRestartStepCompleted(0);

		debugStepPoint(MergeRunnableStopPoint.STEP1_START);

		logger.debug("Start Step 1");

		// if we aren't restarting, create the lock, otherwise switchLock
		// already contains the update lock
		if (!restarting) {
			switchLock = createUpdateLock();
		}
		// create a lock so that new incoming connections don't do anything
		// wait for all running updates to finish
		waitForActiveUpdates();

		debugStepPoint(MergeRunnableStopPoint.STEP1_TEST_BITMAP1);
		debugStepPoint(MergeRunnableStopPoint.STEP1_TEST_SELECT1);

		// init the temp deletes while merging... triples that are deleted while
		// merging might be in the newly generated
		// HDT file
		endpoint.initTempDump(restarting);
		endpoint.initTempDeleteArray();

		debugStepPoint(MergeRunnableStopPoint.STEP1_TEST_BITMAP2);
		debugStepPoint(MergeRunnableStopPoint.STEP1_TEST_SELECT2);

		// mark in the store that the merge process started
		endpoint.setMerging(true);

		sleep(extendsTimeMergeBeginning, "extendsTimeMergeBeginning");
		debugStepPoint(MergeRunnableStopPoint.STEP1_OLD_SLEEP_BEFORE_SWITCH);

		debugStepPoint(MergeRunnableStopPoint.STEP1_TEST_SELECT3);

		// switch the store to freeze it
		this.endpoint.switchStore = !this.endpoint.switchStore;

		debugStepPoint(MergeRunnableStopPoint.STEP1_TEST_SELECT4);

		// reset the count of triples to 0 after switching the stores
		this.endpoint.setTriplesCount(0);

		sleep(extendsTimeMergeBeginningAfterSwitch, "extendsTimeMergeBeginningAfterSwitch");
		debugStepPoint(MergeRunnableStopPoint.STEP1_OLD_SLEEP_AFTER_SWITCH);

		// make a copy of the delete array so that the merge thread doesn't
		// interfere with the store data access @todo:
		// a lock is needed here
		if (restarting) {
			// delete previous array in case of restart
			Files.deleteIfExists(Path.of(endpointFiles.getTripleDeleteCopyArr()));
		}
		Files.copy(Path.of(endpointFiles.getTripleDeleteArr()), Path.of(endpointFiles.getTripleDeleteCopyArr()),
				StandardCopyOption.REPLACE_EXISTING);
		// release the lock so that the connections can continue
		switchLock.release();
		debugStepPoint(MergeRunnableStopPoint.STEP1_END);
		logger.debug("Switch-Lock released");
		logger.info("End merge step 1");

		// @todo: set these operations in an atomic way
		// write the switchStore value to disk in case, something crash we can
		// recover
		this.endpoint.writeWhichStore();
		markRestartStepCompleted(2);
		step2(false, null);
	}

	/**
	 * reload previous data from step 2
	 *
	 * @return previous data of step 2
	 */
	private Lock reloadDataFromStep2() throws IOException {
		// @todo: reload data from step 1
		endpoint.initTempDump(true);
		endpoint.initTempDeleteArray();
		endpoint.setMerging(true);
		return null;
	}

	/**
	 * start the merge at step2
	 *
	 * @param restarting if we are restarting from step 2 or not
	 * @param lock       the return value or {@link #reloadDataFromStep2()}
	 * @throws InterruptedException for wait exception
	 * @throws IOException          for file exception
	 */
	private synchronized void step2(boolean restarting, Lock lock) throws InterruptedException, IOException {
		debugStepPoint(MergeRunnableStopPoint.STEP2_START);
		// diff hdt indexes...
		logger.debug("Dump all triples from the native store to file");
		try (RepositoryConnection nativeStoreConnection = endpoint.getConnectionToFreezedStore()) {
			writeTempFile(nativeStoreConnection, endpointFiles.getRDFTempOutput());
			nativeStoreConnection.commit();
		}
		logger.debug("Create HDT index from dumped file");
		createHDTDump(endpointFiles.getRDFTempOutput(), endpointFiles.getHDTTempOutput());
		// cat the original index and the temp index
		logger.debug("HDT Cat/Diff");
		catDiffIndexes(endpointFiles.getHDTIndex(), endpointFiles.getTripleDeleteCopyArr(),
				endpointFiles.getHDTTempOutput(), endpointFiles.getHDTNewIndex());
		logger.debug("CAT completed!!!!! " + endpointFiles.getLocationHdt());

		debugStepPoint(MergeRunnableStopPoint.STEP2_END);
		markRestartStepCompleted(3);

		// delete the file after the mark if the shutdown occurs during the
		// deletes
		delete(endpointFiles.getRDFTempOutput());
		delete(endpointFiles.getHDTTempOutput());
		delete(endpointFiles.getTripleDeleteCopyArr());
		delete(endpointFiles.getTripleDeleteArr());

		logger.info("End merge step 2");

		step3(false, null);
	}

	/**
	 * return value for {@link #getStep3SubStep()}
	 *
	 * @see #getStep3SubStep()
	 */
	private enum Step3SubStep {
		AFTER_INDEX_V11_RENAME, AFTER_INDEX_RENAME, AFTER_INDEX_V11_OLD_RENAME, AFTER_INDEX_OLD_RENAME,
		AFTER_TRIPLEDEL_TMP_OLD_RENAME, BEFORE_ALL
	}

	/**
	 * @return the sub step of the {@link #step3(boolean, Lock)} method
	 */
	private Step3SubStep getStep3SubStep() {

		boolean existsOldTripleDeleteTempArr = existsOld(endpointFiles.getTripleDeleteTempArr());

		if (!exists(endpointFiles.getHDTNewIndexV11()) && existsOldTripleDeleteTempArr) {
			// after rename(endpointFiles.getHDTNewIndexV11(),
			// endpointFiles.getHDTIndexV11());
			return Step3SubStep.AFTER_INDEX_V11_RENAME;
		}
		if (!exists(endpointFiles.getHDTNewIndex()) && existsOldTripleDeleteTempArr) {
			// after rename(endpointFiles.getHDTNewIndex(),
			// endpointFiles.getHDTIndex());
			return Step3SubStep.AFTER_INDEX_RENAME;
		}
		if (existsOld(endpointFiles.getHDTIndexV11())) {
			// after renameToOld(endpointFiles.getHDTIndexV11());
			return Step3SubStep.AFTER_INDEX_V11_OLD_RENAME;
		}
		if (existsOld(endpointFiles.getHDTIndex())) {
			// after renameToOld(endpointFiles.getHDTIndex());
			return Step3SubStep.AFTER_INDEX_OLD_RENAME;
		}
		if (existsOldTripleDeleteTempArr) {
			// after renameToOld(endpointFiles.getTripleDeleteTempArr());
			return Step3SubStep.AFTER_TRIPLEDEL_TMP_OLD_RENAME;
		}
		return Step3SubStep.BEFORE_ALL;
	}

	/**
	 * preload data for step 3 if restarting
	 */
	private void preloadStep3() {
		deleteIfExists(endpointFiles.getTripleDeleteArr());

		Step3SubStep step3SubStep = getStep3SubStep();

		logger.debug("Reloading step 3 from sub step {}", step3SubStep.name().toLowerCase());

		switch (step3SubStep) {
		case AFTER_INDEX_V11_RENAME:
			rename(endpointFiles.getHDTIndexV11(), endpointFiles.getHDTNewIndexV11());
		case AFTER_INDEX_RENAME:
			rename(endpointFiles.getHDTIndex(), endpointFiles.getHDTNewIndex());
		case AFTER_INDEX_V11_OLD_RENAME:
			renameFromOld(endpointFiles.getHDTIndexV11());
		case AFTER_INDEX_OLD_RENAME:
			renameFromOld(endpointFiles.getHDTIndex());
		case AFTER_TRIPLEDEL_TMP_OLD_RENAME:
			renameFromOld(endpointFiles.getTripleDeleteTempArr());
		case BEFORE_ALL:
			break;
		}
	}

	/**
	 * reload previous data from step 3
	 *
	 * @return previous data of step 3
	 */
	private Lock reloadDataFromStep3() throws IOException {
		endpoint.initTempDump(true);
		endpoint.initTempDeleteArray();
		endpoint.setMerging(true);

		return createConnectionLock();
	}

	/**
	 * start the merge at step3
	 *
	 * @param restarting if we are restarting from step 3 or not
	 * @param lock       the return value or {@link #reloadDataFromStep3()}
	 * @throws InterruptedException for wait exception
	 * @throws IOException          for file exception
	 */
	private synchronized void step3(boolean restarting, Lock lock) throws InterruptedException, IOException {
		logger.debug("Start Step 3");
		debugStepPoint(MergeRunnableStopPoint.STEP3_START);
		// index the new file

		Lock translateLock;

		try (HDT newHdt = HDTManager.mapIndexedHDT(endpointFiles.getHDTNewIndex(), endpoint.getHDTSpec(), null)) {

			// convert all triples added to the merge store to new IDs of the
			// new
			// generated HDT
			logger.debug("ID conversion");
			// create a lock so that new incoming connections don't do anything
			if (!restarting) {
				translateLock = createConnectionLock();
				// wait for all running updates to finish
				waitForActiveConnections();
			} else {
				translateLock = lock;
			}

			this.endpoint.setFreezeNotifications(true);

			this.endpoint.resetDeleteArray(newHdt);
		}

		Path hdtIndexV11 = Path.of(endpointFiles.getHDTIndexV11());
		// if the index.hdt.index.v1-1 doesn't exist, the hdt is empty, so we
		// create a mock index file
		// (ignored by RDF-HDT)
		if (!Files.exists(hdtIndexV11)) {
			Files.writeString(hdtIndexV11, "");
		}

		// rename new hdt to old hdt name so that they are replaces
		// BEFORE_ALL
		renameToOld(endpointFiles.getTripleDeleteTempArr());
		// AFTER_TRIPLEDEL_TMP_OLD_RENAME
		renameToOld(endpointFiles.getHDTIndex());
		debugStepPoint(MergeRunnableStopPoint.STEP3_FILES_MID1);
		// AFTER_INDEX_OLD_RENAME
		renameToOld(endpointFiles.getHDTIndexV11());
		// AFTER_INDEX_V11_OLD_RENAME
		rename(endpointFiles.getHDTNewIndex(), endpointFiles.getHDTIndex());
		debugStepPoint(MergeRunnableStopPoint.STEP3_FILES_MID2);
		// AFTER_INDEX_RENAME
		rename(endpointFiles.getHDTNewIndexV11(), endpointFiles.getHDTIndexV11());
		// AFTER_INDEX_V11_RENAME

		HDT tempHdt = endpoint.loadIndex();

		convertOldToNew(tempHdt);
		this.endpoint.resetHDT(tempHdt, true);

		// mark the triples as deleted from the temp file stored while merge
		this.endpoint.markDeletedTempTriples();
		this.endpoint.setFreezeNotifications(false);
		logger.debug("Releasing lock for ID conversion ....");
		translateLock.release();
		logger.debug("Translate-Lock released");
		logger.debug("Lock released");

		debugStepPoint(MergeRunnableStopPoint.STEP3_END);
		completedMerge();
		// we've deleted the rename marker, we can delete the old HDT
		deleteOld(endpointFiles.getTripleDeleteTempArr());
		deleteOld(endpointFiles.getHDTIndex());
		deleteOld(endpointFiles.getHDTIndexV11());

		debugStepPoint(MergeRunnableStopPoint.MERGE_END);

		this.endpoint.setMerging(false);
		this.endpoint.isMergeTriggered = false;

		debugStepPoint(MergeRunnableStopPoint.MERGE_END_OLD_SLEEP);

		logger.info("Merge finished");
	}

	private void catDiffIndexes(String hdtInput1, String bitArray, String hdtInput2, String hdtOutput)
			throws IOException {
		File file = new File(hdtOutput);
		File theDir = new File(file.getAbsolutePath() + "_tmp");
		Files.createDirectories(theDir.toPath());
		String location = theDir.getAbsolutePath() + File.separator;
		logger.info(location);
		logger.info(hdtInput1);
		logger.info(hdtInput2);
		// @todo: should we not use the already mapped HDT file instead of
		// remapping
		StopWatch sw;
		OverrideHDTOptions catOpt = new OverrideHDTOptions(endpoint.getHDTSpec());
		catOpt.setOverride(HDTOptionsKeys.HDTCAT_LOCATION, location);
		catOpt.setOverride(HDTOptionsKeys.HDTCAT_FUTURE_LOCATION, file.getAbsolutePath());
		try (BitArrayDisk deleteBitmap = new BitArrayDisk(endpoint.getHdt().getTriples().getNumberOfElements(),
				new File(bitArray))) {
			try (HDT hdt = HDTManager.diffBitCatHDT(List.of(hdtInput1, hdtInput2),
					List.of(deleteBitmap, BitmapFactory.empty()), catOpt, null)) {
				sw = new StopWatch();
				// useless to copy the file if it's already there (future
				// location set)
				if (!file.exists()) {
					hdt.saveToHDT(hdtOutput, null);
				}
			}
		}
		logger.info("HDT saved to file in: " + sw.stopAndShow());
	}

	private void createHDTDump(String rdfInput, String hdtOutput) throws IOException {
		String baseURI = Sparql.baseURIFromFilename(rdfInput);
		StopWatch sw = new StopWatch();
		Path location = endpointFiles.getLocationHdtPath().resolve("merger");

		OverrideHDTOptions oopt = new OverrideHDTOptions(this.endpoint.getHDTSpec());
		oopt.setOverride(HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK);
		oopt.setOverride(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, location.resolve("gen"));
		oopt.setOverride(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, location.resolve("wip.hdt"));
		try {
			try (HDT hdt = HDTManager.generateHDT(new File(rdfInput).getAbsolutePath(), baseURI, RDFNotation.NTRIPLES,
					oopt, null)) {
				logger.info("File converted in: " + sw.stopAndShow());
				hdt.saveToHDT(hdtOutput, null);
				logger.info("HDT saved to file in: " + sw.stopAndShow());
			} catch (ParserException e) {
				throw new IOException(e);
			}
		} finally {
			try {
				if (Files.exists(location)) {
					PathUtils.deleteDirectory(location);
				}
			} catch (IOException e) {
				// ignore exception
				e.printStackTrace();
			}
		}
	}

	private void writeTempFile(RepositoryConnection connection, String file) throws IOException {
		try (FileOutputStream out = new FileOutputStream(file)) {
			RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
			RepositoryResult<Statement> repositoryResult = connection.getStatements(null, null, null, false);
			writer.startRDF();
			logger.debug("Content dumped file");
			while (repositoryResult.hasNext()) {
				Statement stm = repositoryResult.next();

				Resource newSubjIRI = this.endpoint.getHdtConverter().rdf4jToHdtIDsubject(stm.getSubject());
				newSubjIRI = this.endpoint.getHdtConverter().subjectHdtResourceToResource(newSubjIRI);

				IRI newPredIRI = this.endpoint.getHdtConverter().rdf4jToHdtIDpredicate(stm.getPredicate());
				newPredIRI = this.endpoint.getHdtConverter().predicateHdtResourceToResource(newPredIRI);

				Value newObjIRI = this.endpoint.getHdtConverter().rdf4jToHdtIDobject(stm.getObject());
				newObjIRI = this.endpoint.getHdtConverter().objectHdtResourceToResource(newObjIRI);

				Statement stmConverted = this.endpoint.getValueFactory().createStatement(newSubjIRI, newPredIRI,
						newObjIRI);
				logger.debug("  {} {} {}", stmConverted.getSubject(), stmConverted.getPredicate(),
						stmConverted.getObject());
				writer.handleStatement(stmConverted);
			}
			writer.endRDF();
		}
	}

	private void convertOldToNew(HDT newHDT) throws IOException {
		logger.info("Started converting IDs in the merge store");
		try {
			Stopwatch stopwatch = Stopwatch.createStarted();
			try (RepositoryConnection connectionChanging = this.endpoint.getConnectionToChangingStore(); // B
					RepositoryConnection connectionFreezed = this.endpoint.getConnectionToFreezedStore() // A
			) {
				connectionFreezed.clear();
				connectionFreezed.begin();
				try (RepositoryResult<Statement> statements = connectionChanging.getStatements(null, null, null)) {
					long count = 0;
					for (Statement s : statements) {
						count++;
						// get the string
						// convert the string using the new dictionary

						// get the old IRIs with old IDs
						HDTConverter iriConverter = new HDTConverter(this.endpoint);
						Resource oldSubject = iriConverter.rdf4jToHdtIDsubject(s.getSubject());
						IRI oldPredicate = iriConverter.rdf4jToHdtIDpredicate(s.getPredicate());
						Value oldObject = iriConverter.rdf4jToHdtIDobject(s.getObject());

						// if the old string cannot be converted than we can
						// keep the
						// same
						Resource newSubjIRI = oldSubject;
						long id = newHDT.getDictionary().stringToId(oldSubject.toString(), TripleComponentRole.SUBJECT);
						if (id != -1) {
							newSubjIRI = iriConverter.subjectIdToIRI(id);
						}

						IRI newPredIRI = oldPredicate;
						id = newHDT.getDictionary().stringToId(oldPredicate.toString(), TripleComponentRole.PREDICATE);

						if (id != -1) {
							newPredIRI = iriConverter.predicateIdToIRI(id);
						}
						Value newObjIRI = oldObject;
						id = newHDT.getDictionary().stringToId(oldObject.toString(), TripleComponentRole.OBJECT);
						if (id != -1) {
							newObjIRI = iriConverter.objectIdToIRI(id);
						}
						logger.debug("old:[{} {} {}]", oldSubject, oldPredicate, oldObject);
						logger.debug("new:[{} {} {}]", newSubjIRI, newPredIRI, newObjIRI);
						connectionFreezed.add(newSubjIRI, newPredIRI, newObjIRI);
						// alternative, i.e. make inplace replacements
						// connectionChanging.remove(s.getSubject(),
						// s.getPredicate(),
						// s.getObject());
						// connectionChanging.add(newSubjIRI, newPredIRI,
						// newObjIRI);

						if (count % MERGE_OLD_TO_NEW_SPLIT == 0) {
							logger.debug("Converted {}", count);
							connectionFreezed.commit();
							connectionFreezed.begin();
						}
					}
					if (count % MERGE_OLD_TO_NEW_SPLIT != 0) {
						connectionFreezed.commit();
					}
				}
				connectionChanging.clear();
			}
			// @todo: why?
			this.endpoint.switchStore = !this.endpoint.switchStore;

			stopwatch.stop(); // optional
			logger.info("Time elapsed for conversion: " + stopwatch);

			// initialize bitmaps again with the new dictionary
			Files.deleteIfExists(Path.of(endpointFiles.getHDTBitX()));
			Files.deleteIfExists(Path.of(endpointFiles.getHDTBitY()));
			Files.deleteIfExists(Path.of(endpointFiles.getHDTBitZ()));
			stopwatch = Stopwatch.createStarted();
			logger.info("Time elapsed to initialize native store dictionary: " + stopwatch);
		} catch (Throwable e) {
			logger.error("Something went wrong during conversion of IDs in merge phase: ");
			throw e;
		}
	}

}

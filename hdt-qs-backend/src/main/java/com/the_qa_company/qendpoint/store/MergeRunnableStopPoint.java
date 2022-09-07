package com.the_qa_company.qendpoint.store;

import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.concurrent.locks.LockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * only for test purpose of {@link MergeRunnable}
 */
public enum MergeRunnableStopPoint {
	/**
	 * debug point at step: at start of step1/merge
	 */
	STEP1_START("at start of step1/merge"),
	/**
	 * debug point at step: step1 test 1
	 */
	STEP1_TEST_BITMAP1("step1 test 1"),
	/**
	 * debug point at step: step 1 test select 1
	 */
	STEP1_TEST_SELECT1("step 1 test select 1"),
	/**
	 * debug point at step: step1 test 2
	 */
	STEP1_TEST_BITMAP2("step1 test 2"),
	/**
	 * debug point at step: step 1 test select 2
	 */
	STEP1_TEST_SELECT2("step 1 test select 2"),
	/**
	 * debug point at step: step 1 test select 3
	 */
	STEP1_TEST_SELECT3("step 1 test select 3"),
	/**
	 * debug point at step: step 1 test select 4
	 */
	STEP1_TEST_SELECT4("step 1 test select 4"),
	/**
	 * debug point at step: step1 old sleep extendsTimeMergeBeginning
	 */
	STEP1_OLD_SLEEP_BEFORE_SWITCH("step1 old sleep extendsTimeMergeBeginning"),
	/**
	 * debug point at step: step1 old sleep extendsTimeMergeBeginningAfterSwitch
	 */
	STEP1_OLD_SLEEP_AFTER_SWITCH("step1 old sleep extendsTimeMergeBeginningAfterSwitch"),
	/**
	 * debug point at step: before step1 marker write
	 */
	STEP1_END("before step1 marker write"),
	/**
	 * debug point at step: at start of step2
	 */
	STEP2_START("at start of step2"),
	/**
	 * debug point at step: before step2 marker write
	 */
	STEP2_END("before step2 marker write"),
	/**
	 * debug point at step: at start of step3
	 */
	STEP3_START("at start of step3"),
	/**
	 * debug point at step: during the rename process part 1
	 */
	STEP3_FILES_MID1("during the rename process part 1"),
	/**
	 * debug point at step: during the rename process part 2
	 */
	STEP3_FILES_MID2("during the rename process part 2"),
	/**
	 * debug point at step: before step3 marker write
	 */
	STEP3_END("before step3 marker write"),
	/**
	 * debug point at step: after merge file deletion
	 */
	MERGE_END("after merge file deletion"),
	/**
	 * debug point at step: after MERGE_END old sleep
	 */
	MERGE_END_OLD_SLEEP("after MERGE_END old sleep");

	private static final Logger logger = LoggerFactory.getLogger(MergeRunnableStopPoint.class);
	// false to simulate a throw, true to simulate an exit
	private static boolean completeFailure = false;
	public static boolean disableRequest = false;
	public static boolean debug = false;
	private static Lock lastLock;
	private static Thread mainThread;

	/**
	 * say we want to halt instead of crashing the merge thread
	 */
	public static void askCompleteFailure() {
		mainThread = Thread.currentThread();
		completeFailure = true;
	}

	public static void unlockAll() {
		for (MergeRunnableStopPoint point : values()) {
			point.debugUnlock();
			point.debugUnlockTest();
		}
	}

	/**
	 * set the last (update/read) lock during the merge
	 *
	 * @param lastLock the last lock created
	 */
	static void setLastLock(Lock lastLock) {
		MergeRunnableStopPoint.lastLock = lastLock;
	}

	/**
	 * an exception during the merge
	 */
	public static class MergeRunnableException extends RuntimeException {
		public MergeRunnableException(String message) {
			super(message);
		}
	}

	/**
	 * an exception triggered by the
	 * {@link MergeRunnable#setStopPoint(MergeRunnableStopPoint)}
	 */
	public class MergeRunnableStopException extends MergeRunnableException {
		public MergeRunnableStopException() {
			super("crashing merge at point: " + name().toLowerCase() + " (" + description + ")");
		}

		/**
		 * @return the stop point of the crash
		 */
		public MergeRunnableStopPoint getStopPoint() {
			return MergeRunnableStopPoint.this;
		}
	}

	/**
	 * the description of the stop point
	 */
	private final String description;
	/**
	 * locker to wait for a step to end
	 */
	private final LockManager lockManager;
	/**
	 * lock to wait for a step to continue
	 */
	private final LockManager lockManagerTest;
	/**
	 * lock to wait for a step to end
	 */
	private Lock lock;
	/**
	 * lock to wait for a step to continue
	 */
	private Lock lockTest;

	MergeRunnableStopPoint(String desc) {
		this.description = desc;
		lockManager = new LockManager();
		lockManagerTest = new LockManager();
	}

	/**
	 * @throws MergeRunnableStopException to crash the MergeRunnable
	 */
	public void debugThrowStop() {
		MergeRunnableStopException e = new MergeRunnableStopException();
		if (completeFailure) {
			logger.error("Complete failure asked, halting the process!!!");
			System.err.println("Merge thread stack trace:");
			e.printStackTrace();
			System.err.println("Main thread stack trace:");
			for (StackTraceElement traceElement : mainThread.getStackTrace())
				System.err.println("\tat " + traceElement);
			Runtime.getRuntime().halt(-1);
		}
		disableRequest = true;
		if (lastLock != null) {
			if (lastLock.isActive()) {
				lastLock.release();
			}
			lastLock = null;
		}
		throw e;
	}

	/**
	 * Debug mode only, wait for this event to be completed, a call to
	 * {@link MergeRunnableStopPoint#debugLock()} should be made before the
	 * merge
	 */
	public void debugWaitForEvent() {
		if (debug) {
			try {
				logger.debug("wait for active lock for event " + name().toLowerCase());
				lockManager.waitForActiveLocks();
				logger.debug("end wait for active lock for event " + name().toLowerCase());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Debug mode only, not for test class!!!, wait for test event to be
	 * completed
	 */
	void debugWaitForTestEvent() {
		if (debug) {
			try {
				lockManagerTest.waitForActiveLocks();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Debug mode only, not for test class!!!, unlock the lock
	 */
	void debugUnlock() {
		if (lock != null) {
			logger.debug("unlock " + name().toLowerCase());
			lock.release();
			lock = null;
		}
	}

	/**
	 * Debug mode only, unlock the lock to continue the merge, a call to
	 * {@link #debugLockTest()} should be made before the merge
	 */
	public void debugUnlockTest() {
		if (lockTest != null) {
			logger.debug("unlock test " + name().toLowerCase());
			lockTest.release();
			lockTest = null;
		}
	}

	/**
	 * Debug mode only, lock this event to be able to wait for it with
	 * {@link #debugWaitForEvent()}
	 */
	public void debugLock() {
		logger.debug("lock " + name().toLowerCase());
		assert lock == null || !lock.isActive();
		lock = lockManager.createLock("stopPointLock_" + name().toLowerCase());
	}

	/**
	 * Debug mode only, lock this event to be able to say when the merge should
	 * continue with {@link #debugUnlockTest()}
	 */
	public void debugLockTest() {
		logger.debug("lock test " + name().toLowerCase());
		assert lockTest == null || !lockTest.isActive();
		lockTest = lockManagerTest.createLock("stopPointLockTest_" + name().toLowerCase());
	}

	/**
	 * @return the description of the stop point
	 */
	public String getDescription() {
		return description;
	}
}

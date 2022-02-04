package com.the_qa_company.q_endpoint.hybridstore;

import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.concurrent.locks.LockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * only for test purpose
 */
public enum MergeRunnableStopPoint {
    STEP1_START("at start of step1/merge"),
    STEP1_LOCK("after the step1 lock"),
    STEP1_END("before step1 marker write"),
    STEP2_START("at start of step2"),
    STEP2_END("before step2 marker write"),
    STEP3_START("at start of step3"),
    STEP3_LOCK("after the step3 lock"),
    STEP3_FILES_MID1("during the rename process part 1"),
    STEP3_FILES_MID2("during the rename process part 2"),
    STEP3_END("before step3 marker write"),
    MERGE_END("after merge file deletion"),
    MERGE_END_AFTER_SLEEP("after MERGE_END and sleep");

    private static final Logger logger = LoggerFactory.getLogger(MergeRunnableStopPoint.class);
    // false to simulate a throw, true to simulate an exit
    public static boolean completeFailure = false;
    public class MergeRunnableStopException extends RuntimeException {
        public MergeRunnableStopException() {
            super("crashing merge at point: "+ name().toLowerCase() + " (" + description + ")");
        }

        public MergeRunnableStopPoint getStopPoint() {
            return MergeRunnableStopPoint.this;
        }
    }

    private String description;
    private LockManager lockManager;
    private LockManager lockManagerTest;
    private Lock lock;
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
        if (completeFailure) {
            logger.error("Complete failure asked, halting the process!!!");
            Runtime.getRuntime().halt(-1);
        }
        throw new MergeRunnableStopException();
    }

    /**
     * Debug mode only, wait for this event to be completed, a call to {@link MergeRunnableStopPoint#debugLock()} should
     * be made before the merge
     */
    public void debugWaitForEvent() {
        if (logger.isDebugEnabled()) {
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
     * Debug mode only, not for test class!!!, wait for test event to be completed
     */
    void debugWaitForTestEvent() {
        if (logger.isDebugEnabled()) {
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
     * Debug mode only, unlock the lock to continue the merge, a call to {@link #debugLockTest()}
     * should be made before the merge
     */
    public void debugUnlockTest() {
        if (lockTest != null) {
            logger.debug("unlock test " + name().toLowerCase());
            lockTest.release();
            lockTest = null;
        }
    }

    /**
     * Debug mode only, lock this event to be able to wait for it with {@link #debugWaitForEvent()}
     */
    public void debugLock() {
        logger.debug("lock " + name().toLowerCase());
        lock = lockManager.createLock("stopPointLock_" + name().toLowerCase());
    }

    /**
     * Debug mode only, lock this event to be able to say when the merge should continue with {@link #debugUnlockTest()}
     */
    public void debugLockTest() {
        logger.debug("lock test " + name().toLowerCase());
        lockTest = lockManagerTest.createLock("stopPointLockTest_" + name().toLowerCase());
    }

    public String getDescription() {
        return description;
    }
}

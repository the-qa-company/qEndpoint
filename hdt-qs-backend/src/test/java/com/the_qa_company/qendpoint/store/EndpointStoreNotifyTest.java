package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.utils.sail.SailTest;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Suite.class)
@Suite.SuiteClasses({ EndpointStoreNotifyTest.EndpointStoreNotifyMergeTest.class, })
public class EndpointStoreNotifyTest {

	public static class EndpointStoreNotifyMergeTest extends AbstractEndpointStoreNotifyTest {
		@Before
		public void setupMerge() {
			MergeRunnableStopPoint.debug = true;
		}

		@After
		public void completeMerge() {
			MergeRunnableStopPoint.debug = false;
		}

		@Test
		public void insertWhileMergingTest() throws InterruptedException {
			MergeRunnableStopPoint.STEP2_START.debugLock();
			MergeRunnableStopPoint.STEP2_START.debugLockTest();

			for (int k = 1; k <= 2; k++) {
				add(mockStmt(0, 0, k));
			}

			// assert the dataset is present
			for (int k = 1; k <= 2; k++) {
				store.assertContains(mockStmt(0, 0, k));
			}

			endpoint.mergeStore();

			MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
			{
				for (int k = 1; k <= 2; k++) {
					store.assertContains(mockStmt(0, 0, k));
				}

				for (int k = 1; k <= 4; k++) {
					add(mockStmt(0, 0, k));
				}

				for (int k = 1; k <= 4; k++) {
					store.assertContains(mockStmt(0, 0, k));
				}
			}
			MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

			MergeRunnable.debugWaitMerge();

			MergeRunnableStopPoint.STEP2_START.debugLock();
			MergeRunnableStopPoint.STEP2_START.debugLockTest();

			for (int k = 1; k <= 4; k++) {
				store.assertContains(mockStmt(0, 0, k));
			}

			for (int k = 1; k <= 6; k++) {
				add(mockStmt(0, 0, k));
			}

			for (int k = 1; k <= 6; k++) {
				store.assertContains(mockStmt(0, 0, k));
			}

			endpoint.mergeStore();

			MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
			{
				for (int k = 1; k <= 6; k++) {
					store.assertContains(mockStmt(0, 0, k));
				}

				for (int k = 1; k <= 8; k++) {
					add(mockStmt(0, 0, k));
				}

				for (int k = 1; k <= 8; k++) {
					store.assertContains(mockStmt(0, 0, k));
				}
			}

			MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

			MergeRunnable.debugWaitMerge();

			for (int k = 1; k <= 8; k++) {
				store.assertContains(mockStmt(0, 0, k));
			}

			for (int k = 1; k <= 10; k++) {
				add(mockStmt(0, 0, k));
			}

			for (int k = 1; k <= 10; k++) {
				store.assertContains(mockStmt(0, 0, k));
			}
		}

		@Test
		public void insertDeleteWhileMergingTest() throws InterruptedException {
			MergeRunnableStopPoint.STEP2_START.debugLock();
			MergeRunnableStopPoint.STEP2_START.debugLockTest();

			for (int k = 1; k <= 2; k++) {
				add(mockStmt(0, 0, k));
			}
			for (int k = 1; k <= 10; k++) {
				add(mockStmt(1, 0, k));
			}

			remove(mockStmt(1, 0, 1));
			// shouldn't do anything
			remove(mockStmt(2, 0, 1));

			// assert the dataset is present
			for (int k = 1; k <= 2; k++) {
				store.assertContains(mockStmt(0, 0, k));
			}

			for (int k = 1; k <= 1; k++) {
				store.assertNotContains(mockStmt(1, 0, k));
			}
			for (int k = 2; k <= 10; k++) {
				store.assertContains(mockStmt(1, 0, k));
			}

			endpoint.mergeStore();

			MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
			{
				for (int k = 1; k <= 2; k++) {
					store.assertContains(mockStmt(0, 0, k));
				}

				for (int k = 1; k <= 1; k++) {
					store.assertNotContains(mockStmt(1, 0, k));
				}

				for (int k = 2; k <= 10; k++) {
					store.assertContains(mockStmt(1, 0, k));
				}

				for (int k = 1; k <= 4; k++) {
					add(mockStmt(0, 0, k));
				}

				remove(mockStmt(1, 0, 2));

				for (int k = 1; k <= 4; k++) {
					store.assertContains(mockStmt(0, 0, k));
				}

				for (int k = 1; k <= 2; k++) {
					store.assertNotContains(mockStmt(1, 0, k));
				}

				for (int k = 3; k <= 10; k++) {
					store.assertContains(mockStmt(1, 0, k));
				}
			}
			MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

			MergeRunnable.debugWaitMerge();

			MergeRunnableStopPoint.STEP2_START.debugLock();
			MergeRunnableStopPoint.STEP2_START.debugLockTest();

			for (int k = 1; k <= 4; k++) {
				store.assertContains(mockStmt(0, 0, k));
			}

			for (int k = 1; k <= 2; k++) {
				store.assertNotContains(mockStmt(1, 0, k));
			}

			for (int k = 3; k <= 10; k++) {
				store.assertContains(mockStmt(1, 0, k));
			}

			for (int k = 1; k <= 6; k++) {
				add(mockStmt(0, 0, k));
			}
			remove(mockStmt(1, 0, 3));

			for (int k = 1; k <= 6; k++) {
				store.assertContains(mockStmt(0, 0, k));
			}

			for (int k = 1; k <= 3; k++) {
				store.assertNotContains(mockStmt(1, 0, k));
			}

			for (int k = 4; k <= 10; k++) {
				store.assertContains(mockStmt(1, 0, k));
			}

			endpoint.mergeStore();

			MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
			{
				for (int k = 1; k <= 6; k++) {
					store.assertContains(mockStmt(0, 0, k));
				}

				for (int k = 1; k <= 3; k++) {
					store.assertNotContains(mockStmt(1, 0, k));
				}

				for (int k = 4; k <= 10; k++) {
					store.assertContains(mockStmt(1, 0, k));
				}

				for (int k = 1; k <= 8; k++) {
					add(mockStmt(0, 0, k));
				}
				remove(mockStmt(1, 0, 4));

				for (int k = 1; k <= 8; k++) {
					store.assertContains(mockStmt(0, 0, k));
				}

				for (int k = 1; k <= 4; k++) {
					store.assertNotContains(mockStmt(1, 0, k));
				}

				for (int k = 5; k <= 10; k++) {
					store.assertContains(mockStmt(1, 0, k));
				}
			}

			MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

			MergeRunnable.debugWaitMerge();

			for (int k = 1; k <= 8; k++) {
				store.assertContains(mockStmt(0, 0, k));
			}

			for (int k = 1; k <= 4; k++) {
				store.assertNotContains(mockStmt(1, 0, k));
			}

			for (int k = 5; k <= 10; k++) {
				store.assertContains(mockStmt(1, 0, k));
			}

			for (int k = 1; k <= 10; k++) {
				add(mockStmt(0, 0, k));
			}
			remove(mockStmt(1, 0, 5));

			for (int k = 1; k <= 10; k++) {
				store.assertContains(mockStmt(0, 0, k));
			}

			for (int k = 1; k <= 5; k++) {
				store.assertNotContains(mockStmt(1, 0, k));
			}

			for (int k = 6; k <= 10; k++) {
				store.assertContains(mockStmt(1, 0, k));
			}
		}
	}

	protected static abstract class AbstractEndpointStoreNotifyTest extends SailTest {
		private final Logger logger = LoggerFactory.getLogger(getClass());
		protected NotifyTestStore store;

		@Override
		protected Sail configStore(EndpointStore endpoint) {
			store = new NotifyTestStore(endpoint);
			return store;
		}

		/**
		 * Store wrapper to check notifying operations
		 *
		 * @author Antoine Willerval
		 */
		protected class NotifyTestStore extends NotifyingSailWrapper implements SailConnectionListener {
			private final Set<Statement> triples = new HashSet<>();

			public NotifyTestStore(EndpointStore baseSail) {
				super(baseSail);
			}

			@Override
			public NotifyingSailConnection getConnection() throws SailException {
				return new NotifyTestStoreConnection(super.getConnection());
			}

			@Override
			public void statementAdded(Statement st) {
				// check if st wasn't already in the store
				assertTrue(st + " was notified to be added, but was in the store!", triples.add(st));
				logger.debug("triple added to store: {}", st);
			}

			@Override
			public void statementRemoved(Statement st) {
				// check if st was in the store
				assertTrue(st + " was notified to be removed, but wasn't in the store!", triples.remove(st));
				logger.debug("triple removed from store: {}", st);
			}

			protected class NotifyTestStoreConnection extends NotifyingSailConnectionWrapper {
				public NotifyTestStoreConnection(NotifyingSailConnection wrappedCon) {
					super(wrappedCon);
					addConnectionListener(NotifyTestStore.this);
				}
			}

			/**
			 * assert the store contains all the statements
			 *
			 * @param statements the statements
			 */
			public void assertContains(Statement... statements) {
				Set<Statement> statementSet = new HashSet<>(Set.of(statements));

				statementSet.removeIf(triples::contains);

				if (!statementSet.isEmpty()) {
					logger.debug("triples: ");
					triples.forEach(t -> logger.debug("- {} ({}) {} ({}) {} ({})", t.getSubject(),
							t.getSubject().getClass(), t.getPredicate(), t.getPredicate().getClass(), t.getObject(),
							t.getObject().getClass()));
					fail("Statements not present: " + statementSet);
				}
			}

			/**
			 * assert the store contains all the statements
			 *
			 * @param statements the statements
			 */
			public void assertNotContains(Statement... statements) {
				Set<Statement> statementSet = new HashSet<>(Set.of(statements));

				statementSet.removeIf(t -> !triples.contains(t));

				if (!statementSet.isEmpty()) {
					logger.debug("triples: ");
					triples.forEach(t -> logger.debug("- {} ({}) {} ({}) {} ({})", t.getSubject(),
							t.getSubject().getClass(), t.getPredicate(), t.getPredicate().getClass(), t.getObject(),
							t.getObject().getClass()));
					fail("Statements present: " + statementSet);
				}

			}
		}
	}

}

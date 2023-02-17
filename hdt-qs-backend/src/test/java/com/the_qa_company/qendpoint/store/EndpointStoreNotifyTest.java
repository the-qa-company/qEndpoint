package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.model.HDTValue;
import com.the_qa_company.qendpoint.utils.sail.SailTest;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.impl.SimpleBinding;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Suite.class)
@Suite.SuiteClasses({ EndpointStoreNotifyTest.MergeTest.class, EndpointStoreNotifyTest.QueryTest.class })
public class EndpointStoreNotifyTest {

	/**
	 * test related with the basic query system
	 */
	public static class QueryTest extends AbstractEndpointStoreNotifyTest {
		@Test
		public void basicQuery() {
			add(VF.createStatement(iri("ex1"), iri("p"), VF.createLiteral("a b c")),
					VF.createStatement(iri("ex2"), iri("p"), VF.createLiteral("b c")),
					VF.createStatement(iri("ex3"), iri("p"), VF.createLiteral("b c c")),
					VF.createStatement(iri("ex4"), iri("p"), VF.createLiteral("c")));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"a\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"b\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"c\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex4"));
		}

		@Test
		public void basicQueryWithDelete() {
			add(VF.createStatement(iri("ex1"), iri("p"), VF.createLiteral("a b c")),
					VF.createStatement(iri("ex2"), iri("p"), VF.createLiteral("b c")),
					VF.createStatement(iri("ex3"), iri("p"), VF.createLiteral("b c c")),
					VF.createStatement(iri("ex4"), iri("p"), VF.createLiteral("c")));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"a\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"b\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"c\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex4"));

			remove(VF.createStatement(iri("ex1"), iri("p"), VF.createLiteral("a b c")));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"a\"", "}"));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"b\"", "}"),
					new SelectResultRow().withIRI("s", "ex2"), new SelectResultRow().withIRI("s", "ex3"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"c\"", "}"),
					new SelectResultRow().withIRI("s", "ex2"), new SelectResultRow().withIRI("s", "ex3"),
					new SelectResultRow().withIRI("s", "ex4"));
		}
	}

	/**
	 * test related with the merge
	 */
	public static class MergeTest extends AbstractEndpointStoreNotifyTest {
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
			remove(mockStmt(1, 0, 1));
			// shouldn't do anything
			remove(mockStmt(2, 0, 1));
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
				remove(mockStmt(1, 0, 2));
				remove(mockStmt(2, 0, 1));

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
			remove(mockStmt(1, 0, 3));
			remove(mockStmt(2, 0, 1));

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
				remove(mockStmt(1, 0, 4));
				remove(mockStmt(2, 0, 1));

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
			remove(mockStmt(1, 0, 5));
			remove(mockStmt(2, 0, 1));

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

		@Test
		public void basicQueryAfterMerge() throws InterruptedException {
			add(VF.createStatement(iri("ex1"), iri("p"), VF.createLiteral("a b c")),
					VF.createStatement(iri("ex2"), iri("p"), VF.createLiteral("b c")),
					VF.createStatement(iri("ex3"), iri("p"), VF.createLiteral("b c c")),
					VF.createStatement(iri("ex4"), iri("p"), VF.createLiteral("c")),
					VF.createStatement(iri("ex5"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex6"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex7"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex8"), iri("p"), iri("placeholder")));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"a\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"b\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"c\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex4"));

			endpoint.mergeStore();

			MergeRunnable.debugWaitMerge();

			try (NotifyingSailConnection co = store.getConnection()) {
				try (CloseableIteration<? extends Statement, SailException> stmt = co.getStatements(iri("ex5"),
						iri("p"), iri("placeholder"), false)) {
					assertTrue(stmt.hasNext());
					assertTrue(stmt.next().getSubject() instanceof HDTValue);
					assertFalse(stmt.hasNext());
				}
			}

			// ex5-8 should be in the hdt
			add(VF.createStatement(iri("ex5"), iri("p"), VF.createLiteral("a b c")),
					VF.createStatement(iri("ex6"), iri("p"), VF.createLiteral("b c")),
					VF.createStatement(iri("ex7"), iri("p"), VF.createLiteral("b c c")),
					VF.createStatement(iri("ex8"), iri("p"), VF.createLiteral("c")));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"a\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex5"));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"b\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex5"),
					new SelectResultRow().withIRI("s", "ex6"), new SelectResultRow().withIRI("s", "ex7"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"c\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex4"),
					new SelectResultRow().withIRI("s", "ex5"), new SelectResultRow().withIRI("s", "ex6"),
					new SelectResultRow().withIRI("s", "ex7"), new SelectResultRow().withIRI("s", "ex8"));
		}

		@Test
		public void basicQueryDuringMerge() throws InterruptedException {
			MergeRunnableStopPoint.STEP2_START.debugLock();
			MergeRunnableStopPoint.STEP2_START.debugLockTest();

			add(VF.createStatement(iri("ex1"), iri("p"), VF.createLiteral("a b c")),
					VF.createStatement(iri("ex2"), iri("p"), VF.createLiteral("b c")),
					VF.createStatement(iri("ex3"), iri("p"), VF.createLiteral("b c c")),
					VF.createStatement(iri("ex4"), iri("p"), VF.createLiteral("c")),
					VF.createStatement(iri("ex5"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex6"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex7"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex8"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex9"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex11"), iri("p"), iri("placeholder")));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"a\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"b\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"c\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex4"));

			endpoint.mergeStore();

			MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
			{
				add(VF.createStatement(iri("ex9"), iri("p"), VF.createLiteral("d")),
						VF.createStatement(iri("ex10"), iri("p"), VF.createLiteral("d")));

				assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"d\"", "}"),
						new SelectResultRow().withIRI("s", "ex9"), new SelectResultRow().withIRI("s", "ex10"));
			}

			MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

			MergeRunnable.debugWaitMerge();

			try (NotifyingSailConnection co = store.getConnection()) {
				try (CloseableIteration<? extends Statement, SailException> stmt = co.getStatements(iri("ex5"),
						iri("p"), iri("placeholder"), false)) {
					assertTrue(stmt.hasNext());
					assertTrue(stmt.next().getSubject() instanceof HDTValue);
					assertFalse(stmt.hasNext());
				}
			}

			// ex5-8 should be in the hdt
			add(VF.createStatement(iri("ex5"), iri("p"), VF.createLiteral("a b c")),
					VF.createStatement(iri("ex6"), iri("p"), VF.createLiteral("b c")),
					VF.createStatement(iri("ex7"), iri("p"), VF.createLiteral("b c c")),
					VF.createStatement(iri("ex8"), iri("p"), VF.createLiteral("c")));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"a\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex5"));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"b\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex5"),
					new SelectResultRow().withIRI("s", "ex6"), new SelectResultRow().withIRI("s", "ex7"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"c\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex4"),
					new SelectResultRow().withIRI("s", "ex5"), new SelectResultRow().withIRI("s", "ex6"),
					new SelectResultRow().withIRI("s", "ex7"), new SelectResultRow().withIRI("s", "ex8"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"d\"", "}"),
					new SelectResultRow().withIRI("s", "ex9"), new SelectResultRow().withIRI("s", "ex10"));

			MergeRunnableStopPoint.STEP2_START.debugLock();
			MergeRunnableStopPoint.STEP2_START.debugLockTest();

			endpoint.mergeStore();

			MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
			{
				add(VF.createStatement(iri("ex11"), iri("p"), VF.createLiteral("e")),
						VF.createStatement(iri("ex12"), iri("p"), VF.createLiteral("e")));

				assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"e\"", "}"),
						new SelectResultRow().withIRI("s", "ex11"), new SelectResultRow().withIRI("s", "ex12"));
			}

			MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

			MergeRunnable.debugWaitMerge();

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"e\"", "}"),
					new SelectResultRow().withIRI("s", "ex11"), new SelectResultRow().withIRI("s", "ex12"));
		}

		@Test
		public void basicQueryDuringMergeWithDelete() throws InterruptedException {
			MergeRunnableStopPoint.STEP2_START.debugLock();
			MergeRunnableStopPoint.STEP2_START.debugLockTest();

			add(VF.createStatement(iri("ex1"), iri("p"), VF.createLiteral("a b c")),
					VF.createStatement(iri("ex2"), iri("p"), VF.createLiteral("b c")),
					VF.createStatement(iri("ex3"), iri("p"), VF.createLiteral("b c c")),
					VF.createStatement(iri("ex4"), iri("p"), VF.createLiteral("c")),
					VF.createStatement(iri("ex5"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex6"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex7"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex8"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex9"), iri("p"), iri("placeholder")),
					VF.createStatement(iri("ex11"), iri("p"), iri("placeholder")));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"a\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"b\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"c\"", "}"),
					new SelectResultRow().withIRI("s", "ex1"), new SelectResultRow().withIRI("s", "ex2"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex4"));

			endpoint.mergeStore();

			MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
			{
				add(VF.createStatement(iri("ex9"), iri("p"), VF.createLiteral("d")),
						VF.createStatement(iri("ex10"), iri("p"), VF.createLiteral("d")));

				remove(VF.createStatement(iri("ex1"), iri("p"), VF.createLiteral("a b c")));

				assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"a\"", "}"));

				assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"d\"", "}"),
						new SelectResultRow().withIRI("s", "ex9"), new SelectResultRow().withIRI("s", "ex10"));
			}

			MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

			MergeRunnable.debugWaitMerge();

			try (NotifyingSailConnection co = store.getConnection()) {
				try (CloseableIteration<? extends Statement, SailException> stmt = co.getStatements(iri("ex5"),
						iri("p"), iri("placeholder"), false)) {
					assertTrue(stmt.hasNext());
					assertTrue(stmt.next().getSubject() instanceof HDTValue);
					assertFalse(stmt.hasNext());
				}
			}

			// ex5-8 should be in the hdt
			add(VF.createStatement(iri("ex5"), iri("p"), VF.createLiteral("a b c")),
					VF.createStatement(iri("ex6"), iri("p"), VF.createLiteral("b c")),
					VF.createStatement(iri("ex7"), iri("p"), VF.createLiteral("b c c")),
					VF.createStatement(iri("ex8"), iri("p"), VF.createLiteral("c")));

			remove(VF.createStatement(iri("ex1"), iri("p"), VF.createLiteral("a b c")));
			remove(VF.createStatement(iri("ex2"), iri("p"), VF.createLiteral("b c")));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"a\"", "}"),
					new SelectResultRow().withIRI("s", "ex5"));

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"b\"", "}"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex5"),
					new SelectResultRow().withIRI("s", "ex6"), new SelectResultRow().withIRI("s", "ex7"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"c\"", "}"),
					new SelectResultRow().withIRI("s", "ex3"), new SelectResultRow().withIRI("s", "ex4"),
					new SelectResultRow().withIRI("s", "ex5"), new SelectResultRow().withIRI("s", "ex6"),
					new SelectResultRow().withIRI("s", "ex7"), new SelectResultRow().withIRI("s", "ex8"));
			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"d\"", "}"),
					new SelectResultRow().withIRI("s", "ex9"), new SelectResultRow().withIRI("s", "ex10"));

			MergeRunnableStopPoint.STEP2_START.debugLock();
			MergeRunnableStopPoint.STEP2_START.debugLockTest();

			endpoint.mergeStore();

			MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
			{
				add(VF.createStatement(iri("ex11"), iri("p"), VF.createLiteral("e")),
						VF.createStatement(iri("ex12"), iri("p"), VF.createLiteral("e")));

				assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"e\"", "}"),
						new SelectResultRow().withIRI("s", "ex11"), new SelectResultRow().withIRI("s", "ex12"));
			}

			MergeRunnableStopPoint.STEP2_START.debugUnlockTest();

			MergeRunnable.debugWaitMerge();

			assertSelect(joinLines("SELECT * {", "?s <" + TEST_STORE_MATCH + "> \"e\"", "}"),
					new SelectResultRow().withIRI("s", "ex11"), new SelectResultRow().withIRI("s", "ex12"));
		}
	}

	protected static abstract class AbstractEndpointStoreNotifyTest extends SailTest {
		/**
		 * basic tuple function in
		 * {@link com.the_qa_company.qendpoint.store.EndpointStoreNotifyTest.AbstractEndpointStoreNotifyTest.NotifyTestStore}
		 * ?subj ex:testStore "query" will search all subject ?subj containing
		 * "query" in a literal (no matter the predicate)
		 */
		protected static final IRI TEST_STORE_MATCH = iri("testStore");
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

				@Override
				public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr,
						Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
					tupleExpr = tupleExpr.clone();
					if (!(tupleExpr instanceof QueryRoot)) {
						tupleExpr = new QueryRoot(tupleExpr);
					}

					// search all the TEST_STORE_MATCH patterns
					QueryVisitorTest queryVisitorTest = new QueryVisitorTest();
					tupleExpr.visit(queryVisitorTest);

					queryVisitorTest.getPatterns().forEach(triplePattern -> {
						// validate and extract subject var name
						Var subjectVar = triplePattern.getSubjectVar();

						if (subjectVar.hasValue()) {
							throw new IllegalArgumentException(
									"SubjectVar of " + TEST_STORE_MATCH + " can't have value!");
						}

						String subject = subjectVar.getName();

						// validate and extract query
						Var objectVar = triplePattern.getObjectVar();

						if (objectVar.hasValue() && !objectVar.getValue().isLiteral()) {
							throw new IllegalArgumentException(
									"ObjectVar of " + TEST_STORE_MATCH + " should have a literal value!");
						}

						String object = objectVar.getValue().stringValue();

						// query the binding
						BindingSetAssignment bsa = new BindingSetAssignment();
						bsa.setBindingNames(triplePattern.getBindingNames());
						bsa.setBindingSets(new TripleSearchBindingSetIterable(subject, object));

						// replace the statement by the binding
						triplePattern.replaceWith(bsa);
					});

					return super.evaluate(tupleExpr, dataset, bindings, includeInferred);
				}

			}

			protected class TripleSearchBindingSetIterable implements Iterable<BindingSet> {
				private final String name;
				private final String query;

				public TripleSearchBindingSetIterable(String name, String query) {
					this.name = name;
					this.query = query;
				}

				@Override
				public Iterator<BindingSet> iterator() {
					return triples.stream()
							.filter(t -> t.getObject().isLiteral() && t.getObject().stringValue().contains(query))
							.map(Statement::getSubject).map(s -> {
								MapBindingSet bindings = new MapBindingSet();
								bindings.addBinding(new SimpleBinding(name, s));
								return (BindingSet) bindings;
							}).collect(Collectors.toList()).iterator();
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

	protected static class QueryVisitorTest extends AbstractQueryModelVisitor<RuntimeException> {
		private final List<StatementPattern> patterns = new ArrayList<>();

		@Override
		public void meet(StatementPattern node) throws RuntimeException {
			if (AbstractEndpointStoreNotifyTest.TEST_STORE_MATCH.equals(node.getPredicateVar().getValue())) {
				patterns.add(node);
			}
		}

		public List<StatementPattern> getPatterns() {
			return patterns;
		}
	}
}

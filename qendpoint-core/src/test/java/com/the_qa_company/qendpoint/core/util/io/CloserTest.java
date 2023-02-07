package com.the_qa_company.qendpoint.core.util.io;

import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CloserTest {

	@Test(expected = AssertionError.class)
	public void closeErrTest() {
		new CloseChecker().check();
	}

	@Test
	public void closeTest() throws IOException {
		CloseChecker checker = new CloseChecker();
		checker.close();
		checker.check();
	}

	@Test
	public void closeAllSingleTest() throws IOException {
		CloseChecker checker = new CloseChecker();

		Closer.closeAll(checker);

		checker.check();
	}

	@Test
	public void closeMultiSingleTest() throws IOException {
		CloseChecker checker = new CloseChecker();
		CloseChecker checker2 = new CloseChecker();
		CloseChecker checker3 = new CloseChecker();

		Closer.closeAll(checker, checker2, checker3);

		checker.check();
		checker2.check();
		checker3.check();
	}

	@Test
	public void closeArrayMultiSingleTest() throws IOException {
		CloseChecker checker = new CloseChecker();
		CloseChecker checker2 = new CloseChecker();
		CloseChecker checker3 = new CloseChecker();
		CloseChecker[] checkArray = { new CloseChecker(), new CloseChecker(), new CloseChecker() };

		Closer.closeAll(checker, checker2, checker3, checkArray);

		checker.check();
		checker2.check();
		checker3.check();

		Arrays.stream(checkArray).forEach(CloseChecker::check);
	}

	@Test
	public void closeListMultiSingleTest() throws IOException {
		CloseChecker checker = new CloseChecker();
		CloseChecker checker2 = new CloseChecker();
		CloseChecker checker3 = new CloseChecker();
		List<CloseChecker> checkArray = Arrays.asList(new CloseChecker(), new CloseChecker(), new CloseChecker());

		Closer.closeAll(checker, checker2, checker3, checkArray);

		checker.check();
		checker2.check();
		checker3.check();

		checkArray.forEach(CloseChecker::check);
	}

	@Test
	public void closeMapMultiSingleTest() throws IOException {
		CloseChecker checker = new CloseChecker();
		CloseChecker checker2 = new CloseChecker();
		CloseChecker checker3 = new CloseChecker();
		Map<String, CloseChecker> checkMap = Map.of("test1", new CloseChecker(), "test2", new CloseChecker(), "test3",
				new CloseChecker());

		Closer.closeAll(checker, checker2, checker3, checkMap);

		checker.check();
		checker2.check();
		checker3.check();

		checkMap.values().forEach(CloseChecker::check);
	}

	@Test
	public void closeAllMultiSingleTest() throws IOException {
		CloseChecker checker = new CloseChecker();
		CloseChecker checker2 = new CloseChecker();
		CloseChecker checker3 = new CloseChecker();
		Map<String, CloseChecker> checkMap = Map.of("test1", new CloseChecker(), "test2", new CloseChecker(), "test3",
				new CloseChecker());
		CloseChecker[] checkArray = { new CloseChecker(), new CloseChecker(), new CloseChecker() };
		List<?> checkList = Arrays.asList(new CloseChecker(), new CloseChecker(), new CloseChecker(), checkArray,
				checkMap);

		Closer.closeAll(checker, checker2, checker3, checkList);

		checker.check();
		checker2.check();
		checker3.check();

		checkMap.values().forEach(CloseChecker::check);
		Arrays.stream(checkArray).forEach(CloseChecker::check);
		checkList.stream().filter(e -> e instanceof CloseChecker).forEach(c -> ((CloseChecker) c).check());
	}

	public static class CloseChecker implements Closeable {
		private boolean closed;

		@Override
		public void close() throws IOException {
			assertFalse("CloseChecker already closed", closed);
			closed = true;
		}

		public boolean isClosed() {
			return closed;
		}

		public void check() {
			assertTrue(closed);
		}
	}
}

package com.the_qa_company.qendpoint.core.util.io;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class CloseSuppressPathTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void createDelTest() throws IOException {
		Path path = tempDir.getRoot().toPath();

		CloseSuppressPath test = CloseSuppressPath.of(path.resolve("test"));

		Files.writeString(test, "test");
		assertTrue(Files.exists(test));
		test.close();
		assertFalse(Files.exists(test));
	}

	@Test
	public void createDelRecTest() throws IOException {
		Path path = tempDir.getRoot().toPath();

		CloseSuppressPath test = CloseSuppressPath.of(path.resolve("test"));
		test.closeWithDeleteRecurse();
		Files.createDirectories(test);

		CloseSuppressPath test2 = test.resolve("test2");
		Files.writeString(test2, "test");
		assertTrue(Files.exists(test));
		assertTrue(Files.exists(test2));
		test.close();
		assertFalse(Files.exists(test2));
		assertFalse(Files.exists(test));
	}

	@Test
	public void pathTest() {
		Path path = tempDir.getRoot().toPath();

		assertEquals(CloseSuppressPath.of(path), path);
		// known unresolvable issue
		assertNotEquals(path, CloseSuppressPath.of(path));
	}
}

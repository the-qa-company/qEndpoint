package com.the_qa_company.qendpoint.core.util.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IOUtilTest {

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testWriteLong() {
		try {
			ByteArrayOutputStream bout = new ByteArrayOutputStream();

			IOUtil.writeLong(bout, 3);
			IOUtil.writeLong(bout, 4);
			IOUtil.writeLong(bout, 0xFF000000000000AAL);
			IOUtil.writeLong(bout, 0x33AABBCCDDEEFF11L);

			ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());

			long a = IOUtil.readLong(bin);
			assertEquals(a, 3);

			long b = IOUtil.readLong(bin);
			assertEquals(b, 4);

			long c = IOUtil.readLong(bin);
			assertEquals(c, 0xFF000000000000AAL);

			long d = IOUtil.readLong(bin);
			assertEquals(d, 0x33AABBCCDDEEFF11L);

		} catch (IOException e) {
			fail("Exception thrown: " + e);
		}
	}

	@Test
	public void testWriteInt() {
		try {
			ByteArrayOutputStream bout = new ByteArrayOutputStream();

			IOUtil.writeInt(bout, 3);
			IOUtil.writeInt(bout, 4);
			IOUtil.writeInt(bout, 0xFF0000AA);
			IOUtil.writeInt(bout, 0xAABBCCDD);

			ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());

			int a = IOUtil.readInt(bin);
			assertEquals(a, 3);

			int b = IOUtil.readInt(bin);
			assertEquals(b, 4);

			int c = IOUtil.readInt(bin);
			assertEquals(c, 0xFF0000AA);

			int d = IOUtil.readInt(bin);
			assertEquals(d, 0xAABBCCDD);

		} catch (IOException e) {
			fail("Exception thrown: " + e);
		}
	}

	@Test(expected = IOException.class)
	public void closeAllSeverity11Test() throws IOException {
		IOUtil.closeAll(() -> { throw new IOException(); }, () -> { throw new IOException(); }, () -> {
			throw new IOException();
		});
	}

	@Test(expected = IOException.class)
	public void closeAllSeverity12Test() throws IOException {
		IOUtil.closeAll((Closeable) () -> { throw new IOException(); });
	}

	@Test(expected = IOException.class)
	public void closeAllSeverity13Test() throws IOException {
		IOUtil.closeAll(() -> { throw new IOException(); }, () -> { throw new IOException(); });
	}

	@Test(expected = RuntimeException.class)
	public void closeAllSeverity2Test() throws IOException {
		IOUtil.closeAll(() -> { throw new IOException(); }, () -> { throw new RuntimeException(); }, () -> {
			throw new IOException();
		});
	}

	@Test(expected = Error.class)
	public void closeAllSeverity3Test() throws IOException {
		IOUtil.closeAll(() -> { throw new Error(); }, () -> { throw new RuntimeException(); }, () -> {
			throw new IOException();
		});
	}

	@Test
	public void closeablePathTest() throws IOException {
		Path p = tempDir.newFolder().toPath();

		Path p1 = p.resolve("test1");
		try (CloseSuppressPath csp = CloseSuppressPath.of(p1)) {
			Files.writeString(csp, "test");
			Assert.assertTrue(Files.exists(p1));
		}
		Assert.assertFalse(Files.exists(p1));

		Path p2 = p.resolve("test2");
		try (CloseSuppressPath csp = CloseSuppressPath.of(p2)) {
			csp.closeWithDeleteRecurse();
			Path p3 = csp.resolve("test3/test4/test5");
			Path f4 = p3.resolve("child.txt");
			Files.createDirectories(p3);
			Files.writeString(f4, "hello world");
			Assert.assertTrue(Files.exists(f4));
		}
		Assert.assertFalse(Files.exists(p2));

	}

	@Ignore("Hand test")
	@Test
	public void urlTest() throws IOException {
		final String url = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.ttl.bz2";

		long len = IOUtil.getContentLengthLong(url);

		assertTrue("bad len: " + len, len > 0);

		System.out.println(len);

		byte[] read;
		final int toRead = 0x1000;
		try (InputStream is = IOUtil.getFileInputStream(url, false)) {
			read = is.readNBytes(toRead);
		}

		assertEquals(toRead, read.length);

		byte[] read2;
		try (InputStream is = IOUtil.getFileInputStream(url, false)) {
			read2 = is.readNBytes(toRead);
		}

		assertArrayEquals(read, read2);

		byte[] read3;
		int midRead = 0x500;
		try (InputStream is = IOUtil.getFileInputStream(url, false)) {
			is.skipNBytes(midRead);
			read3 = is.readNBytes(midRead);
		}
		byte[] read4;
		try (InputStream is = IOUtil.getFileInputStream(url, false, midRead)) {
			read4 = is.readNBytes(midRead);
		}

		assertArrayEquals(read3, read4);

		byte[] read5 = Arrays.copyOfRange(read, midRead, midRead + midRead);

		assertArrayEquals(read3, read5);
	}
}

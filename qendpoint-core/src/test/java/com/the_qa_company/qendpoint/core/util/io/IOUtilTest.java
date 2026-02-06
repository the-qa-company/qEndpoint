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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IOUtilTest {
	private static final class SlowCopyOutputStream extends OutputStream {
		private final byte[] bytes = new byte[Long.BYTES];
		private int index;

		@Override
		public void write(int b) {
			bytes[index++] = (byte) b;
			Thread.yield();
		}

		@Override
		public void write(byte[] b, int off, int len) {
			for (int i = 0; i < len; i++) {
				bytes[index++] = b[off + i];
				Thread.yield();
			}
		}

		byte[] toByteArray() {
			return Arrays.copyOf(bytes, index);
		}
	}

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
	public void testWriteLongConcurrent() throws Exception {
		int threads = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
		int iterations = 20_000;

		ExecutorService workers = Executors.newFixedThreadPool(threads);
		CountDownLatch start = new CountDownLatch(1);
		AtomicInteger mismatches = new AtomicInteger();
		List<Future<Void>> futures = new ArrayList<>(threads);

		try {
			for (int t = 0; t < threads; t++) {
				final int threadId = t;
				futures.add(workers.submit(() -> {
					long seed = 0x1100000000000000L + ((long) threadId << 32);
					start.await();
					for (int i = 0; i < iterations; i++) {
						long expected = seed ^ i;
						SlowCopyOutputStream out = new SlowCopyOutputStream();
						IOUtil.writeLong(out, expected);
						long actual = IOUtil.readLong(new ByteArrayInputStream(out.toByteArray()));
						if (actual != expected) {
							mismatches.incrementAndGet();
							return null;
						}
					}
					return null;
				}));
			}

			start.countDown();
			for (Future<Void> future : futures) {
				future.get();
			}
		} finally {
			workers.shutdownNow();
			assertTrue("workers did not shutdown", workers.awaitTermination(1, TimeUnit.MINUTES));
		}

		assertEquals("concurrent IOUtil.writeLong corrupted bytes", 0, mismatches.get());
	}

	@Test
	public void testWriteLongConcurrentSynchronizedBursts() throws Exception {
		int threads = Math.max(8, Runtime.getRuntime().availableProcessors() * 4);
		int rounds = 4_000;

		ExecutorService workers = Executors.newFixedThreadPool(threads);
		CyclicBarrier syncWrite = new CyclicBarrier(threads);
		AtomicInteger mismatches = new AtomicInteger();
		List<Future<Void>> futures = new ArrayList<>(threads);

		try {
			for (int t = 0; t < threads; t++) {
				final int threadId = t;
				futures.add(workers.submit(() -> {
					long seed = 0x5500000000000000L + ((long) threadId << 32);
					for (int round = 0; round < rounds; round++) {
						long expected = seed + (0x9E3779B97F4A7C15L * round);
						SlowCopyOutputStream out = new SlowCopyOutputStream();

						// Align each writeLong call to maximize overlap across
						// threads.
						syncWrite.await();
						IOUtil.writeLong(out, expected);
						long actual = IOUtil.readLong(new ByteArrayInputStream(out.toByteArray()));
						if (actual != expected) {
							mismatches.incrementAndGet();
						}
					}
					return null;
				}));
			}

			for (Future<Void> future : futures) {
				future.get();
			}
		} finally {
			workers.shutdownNow();
			assertTrue("workers did not shutdown", workers.awaitTermination(1, TimeUnit.MINUTES));
		}

		assertEquals("synchronized bursts corrupted IOUtil.writeLong bytes", 0, mismatches.get());
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

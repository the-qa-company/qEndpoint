package com.the_qa_company.qendpoint.utils;

import org.apache.commons.io.file.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class ChunkInputStreamTest {
	/**
	 * create a random file with a defined length/seed
	 *
	 * @param path   path of the file
	 * @param length length of the file
	 * @param seed   seed of the random
	 * @throws IOException ioe
	 */
	public static void createRandomFile(Path path, long length, long seed) throws IOException {
		assert length > 0;
		Random random = new Random(seed);
		byte[] buffer = new byte[4096];
		long write = 0;
		try (OutputStream stream = Files.newOutputStream(path)) {
			while (write < length) {
				int toWrite = (int) Math.min(buffer.length, length - write);

				random.nextBytes(buffer);

				stream.write(buffer, 0, toWrite);

				write += toWrite;
			}
		}
	}

	public static void assertStreamEquals(InputStream s1, InputStream s2) throws IOException {
		int b1;

		while ((b1 = s1.read()) != -1) {
			assertEquals(b1, s2.read());
		}

		assertEquals(-1, s2.read());
	}

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void randomFileTest() throws IOException {
		Path root = tempDir.getRoot().toPath();

		Path b1 = root.resolve("test1.bin");
		Path b2 = root.resolve("test2.bin");

		try {
			createRandomFile(b1, 284, 9);
			createRandomFile(b2, 284, 9);

			try (InputStream b1s = Files.newInputStream(b1); InputStream b2s = Files.newInputStream(b2)) {
				assertStreamEquals(b1s, b2s);
			}
		} finally {
			Files.deleteIfExists(b1);
			Files.deleteIfExists(b2);
		}
	}

	@Test(expected = AssertionError.class)
	public void randomFileErrorTest() throws IOException {
		Path root = tempDir.getRoot().toPath();

		Path b1 = root.resolve("test1.bin");
		Path b2 = root.resolve("test2.bin");

		try {
			createRandomFile(b1, 284, 9);
			createRandomFile(b2, 285, 9);

			try (InputStream b1s = Files.newInputStream(b1); InputStream b2s = Files.newInputStream(b2)) {
				assertStreamEquals(b1s, b2s);
			}
		} finally {
			Files.deleteIfExists(b1);
			Files.deleteIfExists(b2);
		}
	}

	@Test
	public void streamTest() throws IOException, InterruptedException {
		Path root = tempDir.getRoot().toPath();

		Path file = root.resolve("test.bin");
		try {
			// create random file of 1MB
			createRandomFile(file, 100_000L, 98);

			try (InputStream exceptedStream = Files.newInputStream(file);
					ChunkInputStream chunkStream = new ChunkInputStream(root.resolve("chunks"),
							Files.newInputStream(file), 9999)) {
				assertStreamEquals(exceptedStream, chunkStream);
				chunkStream.joinWorker();
			}
		} finally {
			PathUtils.deleteDirectory(root);
		}

	}

	public String getFileName(URL url) {
		String path = url.getPath();

		int index = path.lastIndexOf('/');
		if (index == -1) {
			return path;
		}

		return path.substring(index + 1);
	}

	@Test
	public void qzd() throws MalformedURLException {
		System.out.println(getFileName(new URL("http://example.org")));
		System.out.println(getFileName(new URL("http://example.org/")));
		System.out.println(getFileName(new URL("http://example.org/test.rdf")));
		System.out.println(getFileName(new URL("http://example.org/dataset/test.rdf")));
		System.out.println(getFileName(new URL("http://example.org/dataset/test.rdf?hello=1")));
	}
}

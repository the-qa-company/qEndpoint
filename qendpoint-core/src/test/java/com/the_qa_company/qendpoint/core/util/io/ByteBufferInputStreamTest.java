package com.the_qa_company.qendpoint.core.util.io;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ByteBufferInputStreamTest {
	private static final long SEED_RND = 0x356772;
	private static final long COUNT = 0x1000;

	private static long writeFile(Path d) throws IOException {
		Random rnd = new Random(SEED_RND);
		try (CountOutputStream os = new CountOutputStream(new BufferedOutputStream(Files.newOutputStream(d)))) {
			for (int i = 0; i < COUNT; i++) {
				IOUtil.writeLong(os, rnd.nextLong());
			}

			byte[] t = new byte[0x1000];

			for (int i = 0; i < COUNT; i++) {
				int bufferLen = rnd.nextInt(t.length);

				for (int j = 0; j < bufferLen; j++) {
					t[j] = (byte) (rnd.nextInt(0x100) & 0xFF);
				}

				os.write(t, 0, bufferLen);
			}
			return os.getTotalBytes();
		}
	}

	private static void testFile(InputStream is) throws IOException {
		Random rnd = new Random(SEED_RND);
		for (int i = 0; i < COUNT; i++) {
			long ex = rnd.nextLong();
			long ac = IOUtil.readLong(is);

			assertEquals("bad number #" + i, ex, ac);
		}
		byte[] ex = new byte[0x1000];
		for (int i = 0; i < COUNT; i++) {

			int bufferLen = rnd.nextInt(ex.length);

			for (int j = 0; j < bufferLen; j++) {
				ex[j] = (byte) (rnd.nextInt(0x100) & 0xFF);
			}

			byte[] ac = is.readNBytes(bufferLen);

			assertArrayEquals(Arrays.copyOf(ex, bufferLen), ac);
		}

	}

	long oldSize;
	int oldSize2;

	@Before
	public void preSetBufferSizes() {
		oldSize = BigMappedByteBuffer.maxBufferSize;
		oldSize2 = BigByteBuffer.maxBufferSize;
		BigMappedByteBuffer.maxBufferSize = COUNT;
		BigByteBuffer.maxBufferSize = (int) COUNT;
	}

	@After
	public void postSetBufferSizes() {
		BigMappedByteBuffer.maxBufferSize = oldSize;
		BigByteBuffer.maxBufferSize = oldSize2;
	}

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void baseTest() throws IOException {
		Path root = tempDir.newFolder().toPath();

		Path testFile = root.resolve("test.bin");

		writeFile(testFile);

		// we test the test
		try (InputStream is = new BufferedInputStream(Files.newInputStream(testFile))) {
			testFile(is);
		}
	}

	@Test
	public void mappedBufferTest() throws IOException {
		Path root = tempDir.newFolder().toPath();

		Path testFile = root.resolve("test.bin");

		long size = writeFile(testFile);

		try (FileChannel ch = FileChannel.open(testFile)) {
			BigMappedByteBuffer buff = BigMappedByteBuffer.ofFileChannel("test.bin", ch, FileChannel.MapMode.READ_ONLY,
					0, size);

			try {
				try (BigMappedByteBufferInputStream is = new BigMappedByteBufferInputStream(buff, 0, size)) {
					testFile(is);
				}
			} finally {
				buff.clean();
			}

		}
	}

	@Test
	public void loadedBufferTest() throws IOException {
		Path root = tempDir.newFolder().toPath();

		Path testFile = root.resolve("test.bin");

		long size = writeFile(testFile);

		BigByteBuffer buffer = BigByteBuffer.allocate(size);

		try (InputStream is = new BufferedInputStream(Files.newInputStream(testFile))) {
			buffer.readStream(is, 0, size, ProgressListener.ignore());
		}

		try (BigByteBufferInputStream is = new BigByteBufferInputStream(buffer, 0, size)) {
			testFile(is);
		}
	}
}

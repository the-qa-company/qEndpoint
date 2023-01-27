package com.the_qa_company.qendpoint.core.util.disk;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;
import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LongArrayDiskTest extends AbstractMapMemoryTest {

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	public static String printByteBuff(ByteBuffer buffer) {
		return IntStream.range(0, buffer.capacity()).mapToObj(buffer::get).map(b -> String.format("%02x", b))
				.collect(Collectors.joining(" "));
	}

	@Test
	public void testClose() throws IOException {
		Path testDir = tempDir.newFolder().toPath();

		Path arraydisk = testDir.resolve("arraydisk");
		int size = 6;

		try (LongArrayDisk array = new LongArrayDisk(arraydisk, size)) {
			for (int i = 0; i < size; i++) {
				array.set(i, i * 2);
				assertEquals("index #" + i, i * 2, array.get(i));
			}
		}

		String f1;

		try (FileChannel channel = FileChannel.open(arraydisk, StandardOpenOption.READ)) {
			ByteBuffer buffer = ByteBuffer.allocate(size * 8);
			channel.read(buffer);
			f1 = printByteBuff(buffer);
		}

		String f2;

		try (FileChannel channel = FileChannel.open(arraydisk, StandardOpenOption.READ, StandardOpenOption.WRITE,
				StandardOpenOption.CREATE);
				CloseMappedByteBuffer closeMappedByteBuffer = IOUtil.mapChannel(arraydisk.toAbsolutePath().toString(),
						channel, FileChannel.MapMode.READ_WRITE, 0, size * 8)) {
			ByteBuffer map = closeMappedByteBuffer.getInternalBuffer();
			try {
				f2 = printByteBuff(map);
			} finally {
				IOUtil.cleanBuffer(map);
			}
		}
		assertEquals(f1, f2);

		try (LongArrayDisk array = new LongArrayDisk(arraydisk, size, false)) {
			for (int i = 0; i < size; i++) {
				assertEquals("index #" + i, i * 2, array.get(i));
			}
		}

		Assert.assertTrue(Files.exists(arraydisk));
		Files.delete(arraydisk);
		Assert.assertFalse(Files.exists(arraydisk));
	}

	@Test
	public void resizeTest() throws IOException {
		Path root = tempDir.getRoot().toPath();

		try (LongArrayDisk array = new LongArrayDisk(root.resolve("file"), 4)) {

			assertEquals(array.length(), 4);

			array.set(0, 1);
			array.set(1, 2);
			array.set(2, 3);
			array.set(3, 4);

			assertEquals(array.get(0), 1);
			assertEquals(array.get(1), 2);
			assertEquals(array.get(2), 3);
			assertEquals(array.get(3), 4);

			array.resize(8);

			assertEquals(array.get(0), 1);
			assertEquals(array.get(1), 2);
			assertEquals(array.get(2), 3);
			assertEquals(array.get(3), 4);
		}
	}

	@Test
	public void clearTest() throws IOException {
		Path root = tempDir.getRoot().toPath();

		long size = 12543;
		try (LongArrayDisk array = new LongArrayDisk(root.resolve("file"), size)) {

			assertEquals(array.length(), size);

			Random r1 = new Random(25);
			for (long i = 0; i < array.length(); i++) {
				array.set(i, r1.nextLong());
			}
			Random r2 = new Random(25);
			for (long i = 0; i < array.length(); i++) {
				assertEquals("#" + i, r2.nextLong(), array.get(i));
			}

			array.clear();

			for (long i = 0; i < array.length(); i++) {
				assertEquals("#" + i, 0, array.get(i));
			}
		}
	}

	@Test
	public void largeResizeTest() throws IOException {
		Path root = tempDir.getRoot().toPath();

		long[] tests = { 7, 5, 9, 2 };

		Path arrFile = root.resolve("arr");
		try {
			long endSize;
			try (LongArrayDisk array = new LongArrayDisk(arrFile, 100)) {
				// add check value after the resizes/loading
				for (int i = 0; i < tests.length; i++) {
					array.set(i, tests[i]);
				}

				// resize from 10^3 to 10^9 to see the usage of multiple
				// mappings in set0
				for (int i = 10; i < 30; i++) {
					array.resize(1L << i);
				}

				// store the size
				endSize = array.length();
			}

			assertEquals("array size and file size aren't matching!", endSize * 8L, Files.size(arrFile));

			long endSize2;

			try (LongArrayDisk array = new LongArrayDisk(arrFile, endSize, false)) {
				// resize to a lower size to test the file truncating
				array.resize(1L << 10);

				// store the new size
				endSize2 = array.length();
			}

			assertEquals("new array size and file size aren't matching!", endSize2 * 8L, Files.size(arrFile));

			assertTrue("old array size isn't bigger than the new size, bad test?", endSize2 < endSize);

			try (LongArrayDisk array = new LongArrayDisk(arrFile, endSize2, false)) {
				// assert that the reloading/resizing didn't cancelled some
				// values
				for (int i = 0; i < tests.length; i++) {
					assertEquals(tests[i], array.get(i));
				}
			}
		} finally {
			Files.deleteIfExists(arrFile);
		}

	}
}

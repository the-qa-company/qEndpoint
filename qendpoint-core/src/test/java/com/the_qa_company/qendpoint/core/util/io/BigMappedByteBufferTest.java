package com.the_qa_company.qendpoint.core.util.io;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import org.junit.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

public class BigMappedByteBufferTest {

	private long normalBufferSize;
	private BigMappedByteBuffer buffer;
	private long size;
	private FileChannel ch;

	private String filename;

	@Before
	public void setup() throws IOException, URISyntaxException {
		normalBufferSize = BigMappedByteBuffer.maxBufferSize;

		Path path = Paths.get(
				Objects.requireNonNull(getClass().getClassLoader().getResource("dbpedia.hdt"), "can't find dbpedia hdt")
						.toURI());

		filename = path.toAbsolutePath().toString();

		size = Files.size(path);

		BigMappedByteBuffer.maxBufferSize = size / 5; // test with huge split

		ch = FileChannel.open(path);
		buffer = BigMappedByteBuffer.ofFileChannel(filename, ch, FileChannel.MapMode.READ_ONLY, 0, size);
	}

	@After
	public void unSetup() throws IOException {
		buffer.clean();
		ch.close();
		BigMappedByteBuffer.maxBufferSize = normalBufferSize;
	}

	@Test
	public void capacityBuffer() {
		List<CloseMappedByteBuffer> buffers = buffer.getBuffers();

		for (int i = 0; i < buffers.size() - 1; i++) {
			int c = buffers.get(i).capacity();
			Assert.assertEquals("Buffer size #" + i, BigMappedByteBuffer.maxBufferSize, c);
		}

		int c = buffers.get(buffers.size() - 1).capacity();

		if (size % BigMappedByteBuffer.maxBufferSize == 0) {
			Assert.assertEquals("Buffer size #" + (buffers.size() - 1), BigMappedByteBuffer.maxBufferSize, c);
		} else {
			Assert.assertEquals("Buffer size #" + (buffers.size() - 1), size % BigMappedByteBuffer.maxBufferSize, c);
		}

	}

	@Test
	public void capacity() {
		Assert.assertEquals("Buffer size", size, buffer.capacity());
	}

	@Test
	public void get() throws IOException {
		BigMappedByteBuffer buffer1 = buffer.duplicate();
		try (CloseMappedByteBuffer buffer2 = IOUtil.mapChannel(filename, ch, FileChannel.MapMode.READ_ONLY, 0, size)) {

			while (buffer2.hasRemaining() && buffer1.hasRemaining()) {
				Assert.assertEquals("position", buffer1.position(), buffer2.position());
				Assert.assertEquals("byte op", buffer1.get(), buffer2.get());
			}

			Assert.assertFalse("Buffer 1 empty", buffer1.hasRemaining());
			Assert.assertFalse("Buffer 2 empty", buffer2.hasRemaining());
		}
	}

	@Test
	public void position() {
		BigMappedByteBuffer test = buffer.duplicate();

		for (long i = 0; i < size; i++) {
			test.position(i);
			Assert.assertEquals("position", i, test.position());
		}
	}

	@Test
	public void getBuff() throws IOException {
		BigMappedByteBuffer buffer1 = buffer.duplicate();
		try (CloseMappedByteBuffer buffer2 = IOUtil.mapChannel(filename, ch, FileChannel.MapMode.READ_ONLY, 0, size)) {

			// add +2 to assure the cross-buffer reading
			byte[] bytes1 = new byte[(int) (BigMappedByteBuffer.maxBufferSize / 2 + 2)];
			byte[] bytes2 = new byte[bytes1.length];

			while (buffer2.hasRemaining() && buffer1.hasRemaining()) {
				int toRead = (int) Math.min(bytes1.length, buffer1.capacity() - buffer2.position());
				buffer1.get(bytes1, 0, toRead);
				buffer2.get(bytes2, 0, toRead);
				Assert.assertArrayEquals("byte op", bytes1, bytes2);
			}

			Assert.assertFalse("Buffer 1 empty", buffer1.hasRemaining());
			Assert.assertFalse("Buffer 2 empty", buffer2.hasRemaining());
		}
	}

	@Test
	@Ignore("HDT git ignored")
	public void largeTest() throws IOException {
		// see
		// https://github.com/rdfhdt/hdt-java/issues/126#issuecomment-1036228969
		try (HDT hdt = HDTManager.mapHDT("/Users/ate/Downloads/archive/dbpedia2016-10.hdt")) {
			System.out.println(hdt.getTriples().getNumberOfElements());
		}
	}
}

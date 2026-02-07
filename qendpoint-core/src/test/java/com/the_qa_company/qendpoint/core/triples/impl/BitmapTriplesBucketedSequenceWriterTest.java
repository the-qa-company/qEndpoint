package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.Lz4Config;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BitmapTriplesBucketedSequenceWriterTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void bucketFilesUseLz4Chunks() throws Exception {
		long totalEntries = 2L;
		int bucketSize = 2;
		int bufferRecords = 4;

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			BucketedSequenceWriter writer = new BucketedSequenceWriter(root, totalEntries, bucketSize, bufferRecords);
			try {
				writer.add(0L, 5L);
				writer.add(1L, 7L);

				DynamicSequence sequence = new SequenceLog64(64, totalEntries, true);
				writer.materializeTo(sequence);

				Path bucketFile;
				try (Stream<Path> paths = Files.list(root)) {
					bucketFile = paths.filter(Files::isRegularFile).findFirst()
							.orElseThrow(() -> new AssertionError("bucket files written"));
				}

				byte[] header = new byte[8];
				try (InputStream in = Files.newInputStream(bucketFile)) {
					int read = in.read(header);
					assertEquals("chunk header length", 8, read);
				}
				int uncompressedLength = readInt(header, 0);
				int compressedLength = readInt(header, Integer.BYTES);
				int expectedUncompressedLength = (Integer.BYTES + Long.BYTES) * 2;
				assertEquals("chunk uncompressed length", expectedUncompressedLength, uncompressedLength);
				assertTrue("chunk compressed length positive", compressedLength > 0);
			} finally {
				writer.close();
			}
		}
	}

	@Test
	public void materializeReportsProgress() throws Exception {
		long totalEntries = 2L;
		int bucketSize = 2;
		int bufferRecords = 4;

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			BucketedSequenceWriter writer = new BucketedSequenceWriter(root, totalEntries, bucketSize, bufferRecords);
			try {
				writer.add(0L, 5L);
				writer.add(1L, 7L);

				List<Float> levels = new ArrayList<>();
				List<String> messages = new ArrayList<>();
				ProgressListener listener = (level, message) -> {
					levels.add(level);
					messages.add(message);
				};

				DynamicSequence sequence = new SequenceLog64(64, totalEntries, true);
				writer.materializeTo(sequence, listener);

				assertTrue("progress listener invoked", !messages.isEmpty());
				assertTrue("progress reached 100", levels.stream().anyMatch(level -> level >= 100f));
			} finally {
				writer.close();
			}
		}
	}

	@Test
	public void addBatchWritesRecords() throws Exception {
		long totalEntries = 4L;
		int bucketSize = 2;
		int bufferRecords = 4;

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			BucketedSequenceWriter writer = new BucketedSequenceWriter(root, totalEntries, bucketSize, bufferRecords);
			try {
				long[] indexes = { 0L, 1L, 2L, 3L };
				long[] values = { 9L, 7L, 5L, 3L };
				writer.addBatch(indexes, values, indexes.length);

				DynamicSequence sequence = new SequenceLog64(64, totalEntries, true);
				writer.materializeTo(sequence);

				assertEquals("value 0", 9L, sequence.get(0));
				assertEquals("value 1", 7L, sequence.get(1));
				assertEquals("value 2", 5L, sequence.get(2));
				assertEquals("value 3", 3L, sequence.get(3));
			} finally {
				writer.close();
			}
		}
	}

	@Test
	public void parallelBucketWritesAvoidCommonPool() throws Exception {
		long totalEntries = 20_000L;
		int bucketSize = 1024;
		int bufferRecords = 20_000;

		Class<?> observerType = Class
				.forName("com.the_qa_company.qendpoint.core.triples.impl.BucketedSequenceWriter$BucketWriteObserver");
		Method setter = BucketedSequenceWriter.class.getDeclaredMethod("setBucketWriteObserver", observerType);
		setter.setAccessible(true);

		Set<String> threads = ConcurrentHashMap.newKeySet();
		Object observer = Proxy.newProxyInstance(observerType.getClassLoader(), new Class[] { observerType },
				(proxy, method, args) -> {
					threads.add((String) args[1]);
					return null;
				});

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			BucketedSequenceWriter writer = new BucketedSequenceWriter(root, totalEntries, bucketSize, bufferRecords);
			try {
				writer.setWriteParallelism(4);
				setter.invoke(null, observer);

				long[] indexes = new long[(int) totalEntries];
				long[] values = new long[(int) totalEntries];
				for (int i = 0; i < totalEntries; i++) {
					indexes[i] = i;
					values[i] = i;
				}
				writer.addBatch(indexes, values, indexes.length);

				DynamicSequence sequence = new SequenceLog64(64, totalEntries, true);
				writer.materializeTo(sequence);
			} finally {
				setter.invoke(null, new Object[] { null });
				writer.close();
			}
		}

		assertTrue("parallel writes used fork-join workers",
				threads.stream().anyMatch(name -> name.startsWith("ForkJoinPool-")));
		assertTrue("parallel writes avoided common pool",
				threads.stream().noneMatch(name -> name.contains("commonPool")));
	}

	@Test
	public void bucketFilesWriteRawChunksWhenLz4Disabled() throws Exception {
		boolean original = Lz4Config.isEnabled();
		try {
			Lz4Config.setEnabled(false);
			long totalEntries = 2L;
			int bucketSize = 2;
			int bufferRecords = 4;

			try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
				root.closeWithDeleteRecurse();

				BucketedSequenceWriter writer = new BucketedSequenceWriter(root, totalEntries, bucketSize,
						bufferRecords);
				try {
					writer.add(0L, 5L);
					writer.add(1L, 7L);

					DynamicSequence sequence = new SequenceLog64(64, totalEntries, true);
					writer.materializeTo(sequence);

					Path bucketFile;
					try (Stream<Path> paths = Files.list(root)) {
						bucketFile = paths.filter(Files::isRegularFile).findFirst()
								.orElseThrow(() -> new AssertionError("bucket files written"));
					}

					byte[] header = new byte[8];
					try (InputStream in = Files.newInputStream(bucketFile)) {
						int read = in.read(header);
						assertEquals("chunk header length", 8, read);
					}
					int compressedLength = readInt(header, Integer.BYTES);
					assertTrue("raw chunk sentinel", compressedLength < 0);
				} finally {
					writer.close();
				}
			}
		} finally {
			Lz4Config.setEnabled(original);
		}
	}

	private static int readInt(byte[] buffer, int offset) {
		return ((buffer[offset] & 0xff) << 24) | ((buffer[offset + 1] & 0xff) << 16)
				| ((buffer[offset + 2] & 0xff) << 8) | (buffer[offset + 3] & 0xff);
	}

}

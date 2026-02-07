package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.dictionary.impl.CompressFourSectionDictionary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BucketedTripleMapperMaterializationTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void materializesMappingsIntoCompressTripleMapper() throws Exception {
		Class<?> bucketedMapperClass = Class
				.forName("com.the_qa_company.qendpoint.core.hdt.impl.diskimport.BucketedTripleMapper");

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			long tripleCount = 4;
			try (CloseSuppressPath bucketDir = root.resolve("bucketed")) {
				bucketDir.mkdirs();

				Object bucketed = bucketedMapperClass
						.getConstructor(CloseSuppressPath.class, long.class, boolean.class, int.class, int.class)
						.newInstance(bucketDir, tripleCount, false, 2, 16);
				CompressFourSectionDictionary.NodeConsumer consumer = (CompressFourSectionDictionary.NodeConsumer) bucketed;

				for (long tripleId = 1; tripleId <= tripleCount; tripleId++) {
					long headerId = CompressUtil.getHeaderId(tripleId);
					consumer.onSubject(tripleId, headerId);
					consumer.onPredicate(tripleId, headerId);
					consumer.onObject(tripleId, headerId);
				}

				try (CloseSuppressPath mapperDir = root.resolve("mapper")) {
					mapperDir.mkdirs();

					CompressTripleMapper mapper = new CompressTripleMapper(mapperDir, tripleCount, 1024, false, 0);
					List<String> messages = new CopyOnWriteArrayList<>();
					List<Float> levels = new CopyOnWriteArrayList<>();
					ProgressListener progressListener = (level, message) -> {
						levels.add(level);
						messages.add(message);
					};

					Method materializeTo = bucketedMapperClass.getMethod("materializeTo", CompressTripleMapper.class,
							ProgressListener.class);
					materializeTo.invoke(bucketed, mapper, progressListener);
					mapper.setShared(0);

					assertTrue("progress listener invoked", !messages.isEmpty());
					assertTrue("subject progress reported",
							messages.stream().anyMatch(message -> message.contains("Materialized SUBJECT mapping")));
					assertTrue("predicate progress reported",
							messages.stream().anyMatch(message -> message.contains("Materialized PREDICATE mapping")));
					assertTrue("object progress reported",
							messages.stream().anyMatch(message -> message.contains("Materialized OBJECT mapping")));
					assertTrue("progress reached 100", levels.stream().anyMatch(level -> level >= 100f));

					for (long tripleId = 1; tripleId <= tripleCount; tripleId++) {
						assertEquals("subject mapping", tripleId, mapper.extractSubject(tripleId));
						assertEquals("predicate mapping", tripleId, mapper.extractPredicate(tripleId));
						assertEquals("object mapping", tripleId, mapper.extractObjects(tripleId));
					}

					mapper.delete();
				}

				Method close = bucketedMapperClass.getMethod("close");
				close.invoke(bucketed);
			}
		}
	}

	@Test
	public void bucketFilesUseLz4Chunks() throws Exception {
		Class<?> bucketedMapperClass = Class
				.forName("com.the_qa_company.qendpoint.core.hdt.impl.diskimport.BucketedTripleMapper");

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			long tripleCount = 4;
			try (CloseSuppressPath bucketDir = root.resolve("bucketed")) {
				bucketDir.mkdirs();

				Object bucketed = bucketedMapperClass
						.getConstructor(CloseSuppressPath.class, long.class, boolean.class, int.class, int.class)
						.newInstance(bucketDir, tripleCount, false, 2, 16);
				CompressFourSectionDictionary.NodeConsumer consumer = (CompressFourSectionDictionary.NodeConsumer) bucketed;

				for (long tripleId = 1; tripleId <= tripleCount; tripleId++) {
					long headerId = CompressUtil.getHeaderId(tripleId);
					consumer.onSubject(tripleId, headerId);
					consumer.onPredicate(tripleId, headerId);
					consumer.onObject(tripleId, headerId);
				}

				try (CloseSuppressPath mapperDir = root.resolve("mapper")) {
					mapperDir.mkdirs();

					CompressTripleMapper mapper = new CompressTripleMapper(mapperDir, tripleCount, 1024, false, 0);
					Method materializeTo = bucketedMapperClass.getMethod("materializeTo", CompressTripleMapper.class);
					materializeTo.invoke(bucketed, mapper);
					mapper.delete();
				}

				Path subjectsDir = bucketDir.resolve("subjects");
				Path bucketFile;
				try (Stream<Path> paths = Files.list(subjectsDir)) {
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

				Method close = bucketedMapperClass.getMethod("close");
				close.invoke(bucketed);
			}
		}
	}

	@Test
	public void materializesRolesInParallelThreads() throws Exception {
		Class<?> bucketedMapperClass = Class
				.forName("com.the_qa_company.qendpoint.core.hdt.impl.diskimport.BucketedTripleMapper");
		Class<?> observerType = Class.forName(
				"com.the_qa_company.qendpoint.core.hdt.impl.diskimport.BucketedTripleMapper$MaterializationObserver");
		Method setObserver = bucketedMapperClass.getDeclaredMethod("setMaterializationObserver", observerType);
		setObserver.setAccessible(true);

		Map<String, String> roleThreads = new ConcurrentHashMap<>();
		Object observer = Proxy.newProxyInstance(observerType.getClassLoader(), new Class[] { observerType },
				(proxy, method, args) -> {
					roleThreads.put((String) args[0], (String) args[1]);
					return null;
				});

		try {
			setObserver.invoke(null, observer);

			try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
				root.closeWithDeleteRecurse();

				long tripleCount = 100;
				try (CloseSuppressPath bucketDir = root.resolve("bucketed")) {
					bucketDir.mkdirs();

					Object bucketed = bucketedMapperClass
							.getConstructor(CloseSuppressPath.class, long.class, boolean.class, int.class, int.class)
							.newInstance(bucketDir, tripleCount, false, 8, 64);
					CompressFourSectionDictionary.NodeConsumer consumer = (CompressFourSectionDictionary.NodeConsumer) bucketed;

					for (long tripleId = 1; tripleId <= tripleCount; tripleId++) {
						long headerId = CompressUtil.getHeaderId(tripleId);
						consumer.onSubject(tripleId, headerId);
						consumer.onPredicate(tripleId, headerId);
						consumer.onObject(tripleId, headerId);
					}

					try (CloseSuppressPath mapperDir = root.resolve("mapper")) {
						mapperDir.mkdirs();

						CompressTripleMapper mapper = new CompressTripleMapper(mapperDir, tripleCount, 1024, false, 0);
						Method materializeTo = bucketedMapperClass.getMethod("materializeTo",
								CompressTripleMapper.class);
						materializeTo.invoke(bucketed, mapper);
						mapper.delete();
					}

					Method close = bucketedMapperClass.getMethod("close");
					close.invoke(bucketed);
				}
			}
		} finally {
			setObserver.invoke(null, new Object[] { null });
		}

		assertEquals("subject thread", true, roleThreads.containsKey("SUBJECT"));
		assertEquals("predicate thread", true, roleThreads.containsKey("PREDICATE"));
		assertEquals("object thread", true, roleThreads.containsKey("OBJECT"));
		assertTrue("materialize threads have prefix",
				roleThreads.values().stream().allMatch(name -> name.startsWith("BucketedTripleMapper-materialize-")));
		assertTrue("multiple role threads used", roleThreads.values().stream().distinct().count() >= 3);
	}

	private static int readInt(byte[] buffer, int offset) {
		return ((buffer[offset] & 0xff) << 24) | ((buffer[offset + 1] & 0xff) << 16)
				| ((buffer[offset + 2] & 0xff) << 8) | (buffer[offset + 3] & 0xff);
	}
}

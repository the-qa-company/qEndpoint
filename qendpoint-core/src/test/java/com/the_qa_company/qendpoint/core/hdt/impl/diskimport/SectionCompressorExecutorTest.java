package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.iterator.utils.SizedSupplier;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SectionCompressorExecutorTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void computeUsesSharedFixedThreadPool() throws Exception {
		ThreadPoolExecutor executor = sharedExecutor();
		int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
		assertEquals(cores, executor.getCorePoolSize());
		assertEquals(cores, executor.getMaximumPoolSize());

		Set<String> mergeThreads = new ConcurrentSkipListSet<>();
		MultiThreadListener listener = (thread, level, message) -> mergeThreads.add(thread);

		try (CloseSuppressPath tmp = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			tmp.closeWithDeleteRecurse();

			int bufferSize = 16 * 1024;
			long chunkSize = 1024L;
			int k = 2;

			SectionCompressor compressor = new SectionCompressor(tmp.resolve("base"), listener, bufferSize, chunkSize,
					k, false, false, CompressionType.NONE);

			CloseSuppressPath chunk1 = tmp.resolve("chunk1");
			CloseSuppressPath chunk2 = tmp.resolve("chunk2");
			CloseSuppressPath merged = tmp.resolve("merged");

			compressor.createChunk(
					supplierOf(Arrays.asList(new TripleString("b", "p1", "o1"), new TripleString("a", "p2", "o2"))),
					chunk1);
			compressor.createChunk(
					supplierOf(Arrays.asList(new TripleString("c", "p3", "o3"), new TripleString("d", "p4", "o4"))),
					chunk2);

			compressor.mergeChunks(List.of(chunk1, chunk2), merged);
		}

		boolean usedMergeExecutor = mergeThreads.stream()
				.anyMatch(name -> name.startsWith("section-compressor-merge-"));
		assertTrue("expected shared merge executor to run tasks", usedMergeExecutor);
	}

	private static ThreadPoolExecutor sharedExecutor() {
		try {
			Field field = SectionCompressor.class.getDeclaredField("MERGE_EXECUTOR");
			field.setAccessible(true);
			Object value = field.get(null);
			assertTrue("MERGE_EXECUTOR should be a ThreadPoolExecutor", value instanceof ThreadPoolExecutor);
			return (ThreadPoolExecutor) value;
		} catch (ReflectiveOperationException e) {
			throw new AssertionError("expected SectionCompressor to expose MERGE_EXECUTOR", e);
		}
	}

	private static SizedSupplier<TripleString> supplierOf(List<TripleString> triples) {
		Iterator<TripleString> iterator = triples.iterator();
		return new SizedSupplier<>() {
			@Override
			public TripleString get() {
				return iterator.hasNext() ? iterator.next() : null;
			}

			@Override
			public long getSize() {
				return 0;
			}
		};
	}
}

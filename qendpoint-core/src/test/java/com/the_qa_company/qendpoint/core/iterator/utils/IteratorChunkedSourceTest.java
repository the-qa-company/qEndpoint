package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class IteratorChunkedSourceTest {
	@Test
	public void chunksByBudget() throws Exception {
		List<Integer> input = IntStream.range(0, 10).boxed().collect(Collectors.toList());
		IteratorChunkedSource<Integer> source = IteratorChunkedSource.of(input.iterator(), v -> 1, 3, null);

		List<Integer> out = new ArrayList<>();
		SizedSupplier<Integer> chunk;
		while ((chunk = source.get()) != null) {
			assertTrue("Chunk size should be > 0", chunk.getSize() > 0);
			Integer v;
			while ((v = chunk.get()) != null) {
				out.add(v);
			}
		}

		assertEquals(input, out);
	}

	@Test
	public void concurrentGetTerminates() throws Exception {
		List<Integer> input = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
		IteratorChunkedSource<Integer> source = IteratorChunkedSource.of(input.iterator(), v -> 1, 17, null);

		int threads = 8;
		ConcurrentLinkedQueue<Integer> out = new ConcurrentLinkedQueue<>();
		CountDownLatch done = new CountDownLatch(threads);

		for (int i = 0; i < threads; i++) {
			Thread t = new Thread(() -> {
				try {
					SizedSupplier<Integer> chunk;
					while ((chunk = source.get()) != null) {
						Integer v;
						while ((v = chunk.get()) != null) {
							out.add(v);
						}
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				} finally {
					done.countDown();
				}
			});
			t.start();
		}

		assertTrue("Workers should finish", done.await(10, TimeUnit.SECONDS));

		Set<Integer> uniq = new HashSet<>(out);
		assertEquals("No duplicates expected", uniq.size(), out.size());
		assertEquals(new HashSet<>(input), uniq);
	}
}

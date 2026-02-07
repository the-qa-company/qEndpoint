package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class CallbackChunkedSourceTest {
	@Test
	public void concurrentConsumersTerminate() throws Exception {
		CallbackChunkedSource<Integer> source = CallbackChunkedSource.start(sink -> {
			for (int i = 0; i < 1000; i++) {
				sink.accept(i);
			}
		}, v -> 1, 25, Integer.MAX_VALUE, 2);

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
		assertEquals(1000, uniq.size());
		for (int i = 0; i < 1000; i++) {
			assertTrue(uniq.contains(i));
		}

		source.close();
	}
}

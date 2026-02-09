package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class LoserTreeMergeExceptionIteratorProgressTest {

	@Test
	public void notifReportsProgressWhenMergedWithLoserTree() throws Exception {
		ExceptionIterator<Integer, RuntimeException> it1 = ExceptionIterator.of(List.of(1, 3).iterator());
		ExceptionIterator<Integer, RuntimeException> it2 = ExceptionIterator.of(List.of(2, 4).iterator());

		AtomicInteger notifications = new AtomicInteger();
		ProgressListener listener = (level, message) -> notifications.incrementAndGet();

		ExceptionIterator<Integer, RuntimeException> merged = LoserTreeMergeExceptionIterator
				.merge(List.of(it1, it2), Integer::compareTo).notif(4, 20, "Merge", listener);

		while (merged.hasNext()) {
			merged.next();
		}

		assertTrue("Expected progress notifications for merged iterator", notifications.get() > 0);
	}
}

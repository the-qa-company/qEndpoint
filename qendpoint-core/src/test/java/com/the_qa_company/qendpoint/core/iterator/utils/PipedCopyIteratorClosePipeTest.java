package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class PipedCopyIteratorClosePipeTest {
	@Test
	public void closePipeWithExceptionClearsQueuedElements() {
		PipedCopyIterator<Integer> pipe = new PipedCopyIterator<>(1);
		pipe.addElement(1);

		pipe.closePipe(new RuntimeException("boom"));

		try {
			pipe.hasNext();
			fail("Expected closing exception");
		} catch (PipedCopyIterator.PipedIteratorException e) {
			assertEquals("closing exception", e.getMessage());
			assertNotNull(e.getCause());
			assertEquals("boom", e.getCause().getMessage());
		}
	}
}

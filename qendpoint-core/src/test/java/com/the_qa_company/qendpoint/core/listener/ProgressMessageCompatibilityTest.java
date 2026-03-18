package com.the_qa_company.qendpoint.core.listener;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class ProgressMessageCompatibilityTest {
	@Test
	public void progressMessageFormatsTemplate() {
		ProgressMessage message = ProgressMessage.format("a{}b{}", "X", 2);
		String rendered = message.render();
		assertEquals("aXb2", rendered);
	}

	@Test
	public void progressListenerTemplateOverloadFormatsMessage() {
		AtomicReference<String> captured = new AtomicReference<>();
		ProgressListener listener = (level, message) -> captured.set(message);
		listener.notifyProgress(12.5f, "hello {} {}", "A", 5);

		assertEquals("hello A 5", captured.get());
	}
}

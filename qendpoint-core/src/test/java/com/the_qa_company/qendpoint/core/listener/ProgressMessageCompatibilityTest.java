package com.the_qa_company.qendpoint.core.listener;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class ProgressMessageCompatibilityTest {
	@Test
	public void progressMessageFormatsTemplate() throws Exception {
		Class<?> messageClass = Class.forName("com.the_qa_company.qendpoint.core.listener.ProgressMessage");
		Method format = messageClass.getMethod("format", String.class, Object[].class);
		Object message = format.invoke(null, "a{}b{}", new Object[] { "X", 2 });
		Method render = messageClass.getMethod("render");
		String rendered = (String) render.invoke(message);
		assertEquals("aXb2", rendered);
	}

	@Test
	public void progressListenerTemplateOverloadFormatsMessage() throws Exception {
		AtomicReference<String> captured = new AtomicReference<>();
		ProgressListener listener = (level, message) -> captured.set(message);

		Method notifyTemplate = ProgressListener.class.getMethod("notifyProgress", float.class, String.class,
				Object[].class);
		notifyTemplate.invoke(listener, 12.5f, "hello {} {}", new Object[] { "A", 5 });

		assertEquals("hello A 5", captured.get());
	}
}

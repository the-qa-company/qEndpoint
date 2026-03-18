package com.the_qa_company.qendpoint.core.util.listener;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MultiThreadListenerConsoleCompatibilityTest {
	@Test
	public void consoleUsesTreeMapAndLegacyRefresh() throws Exception {
		MultiThreadListenerConsole console = new MultiThreadListenerConsole(false, true);

		Field messagesField = MultiThreadListenerConsole.class.getDeclaredField("threadMessages");
		messagesField.setAccessible(true);
		Object messages = messagesField.get(console);
		assertNotNull("threadMessages should be allocated", messages);
		assertTrue("threadMessages should be a TreeMap", messages instanceof TreeMap);

		Field refreshField = MultiThreadListenerConsole.class.getDeclaredField("REFRESH_MILLIS");
		refreshField.setAccessible(true);
		assertEquals(300L, refreshField.getLong(null));
	}

	@Test
	public void consoleWithoutAsciiListenerDisablesThreadMessageBuffer() throws Exception {
		MultiThreadListenerConsole console = new MultiThreadListenerConsole(false, false);

		Field messagesField = MultiThreadListenerConsole.class.getDeclaredField("threadMessages");
		messagesField.setAccessible(true);
		Object messages = messagesField.get(console);
		assertNull("threadMessages should be disabled", messages);
	}
}

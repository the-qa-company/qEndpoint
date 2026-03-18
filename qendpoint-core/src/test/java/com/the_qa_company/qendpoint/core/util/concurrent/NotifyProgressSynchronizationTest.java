package com.the_qa_company.qendpoint.core.util.concurrent;

import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class NotifyProgressSynchronizationTest {
	@Test
	public void syncListenerNotifyProgressIsNotSynchronized() throws Exception {
		Method method = SyncListener.class.getDeclaredMethod("notifyProgress", float.class, String.class);
		Assert.assertFalse("SyncListener.notifyProgress must not be synchronized",
				Modifier.isSynchronized(method.getModifiers()));
	}

//	@Test
//	public void multiThreadListenerConsoleNotifyProgressIsNotSynchronized() throws Exception {
//		Method method = MultiThreadListenerConsole.class.getDeclaredMethod("notifyProgress", String.class, float.class,
//				String.class);
//		Assert.assertFalse("MultiThreadListenerConsole.notifyProgress must not be synchronized",
//				Modifier.isSynchronized(method.getModifiers()));
//	}
}

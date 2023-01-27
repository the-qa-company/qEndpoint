package com.the_qa_company.qendpoint.core.util.listener;

import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;

/**
 * Simple {@link ProgressListener} implementation concatenating a prefix to each
 * message
 *
 * @author Antoine Willerval
 */
public abstract class PrefixListener implements ProgressListener {
	private static class SingleThreadPrefixListener extends PrefixListener {
		private final ProgressListener listener;

		private SingleThreadPrefixListener(String prefix, ProgressListener listener) {
			super(prefix);
			this.listener = listener;
		}

		@Override
		public void clearThreads() {
			// do nothing
		}

		@Override
		public void notifyProgress(float level, String message) {
			listener.notifyProgress(level, prefix + message);
		}
	}

	private static class MultiThreadPrefixListener extends PrefixListener implements MultiThreadListener {
		private final MultiThreadListener listener;

		private MultiThreadPrefixListener(String prefix, MultiThreadListener listener) {
			super(prefix);
			this.listener = listener;
		}

		@Override
		public void notifyProgress(String thread, float level, String message) {
			listener.notifyProgress(thread, level, prefix + message);
		}

		@Override
		public void unregisterAllThreads() {
			listener.unregisterAllThreads();
		}

		@Override
		public void clearThreads() {
			unregisterAllThreads();
		}

		@Override
		public void registerThread(String threadName) {
			listener.registerThread(threadName);
		}

		@Override
		public void unregisterThread(String threadName) {
			listener.unregisterThread(threadName);
		}
	}

	/**
	 * create a prefix listener from another listener, allow multi-thread
	 * listener
	 *
	 * @param prefix   prefix to concat to the messages
	 * @param listener the listener
	 * @return null if listener is null, listener if prefix is null or empty or
	 *         a prefix listener
	 */
	public static PrefixListener of(String prefix, ProgressListener listener) {
		if (listener == null) {
			return null;
		}

		if (listener instanceof MultiThreadListener) {
			return new MultiThreadPrefixListener(prefix, (MultiThreadListener) listener);
		} else {
			return new SingleThreadPrefixListener(prefix, listener);
		}
	}

	protected final String prefix;

	private PrefixListener(String prefix) {
		this.prefix = prefix;
	}

	public abstract void clearThreads();
}

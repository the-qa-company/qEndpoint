package com.the_qa_company.qendpoint.core.listener;

import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;

/**
 * version of {@link ProgressListener} for multi-thread logging
 */
@FunctionalInterface
public interface MultiThreadListener extends ProgressListener {
	static MultiThreadListener ofSingle(ProgressListener listener) {
		if (listener instanceof MultiThreadListener mtl) {
			return mtl;
		}
		if (listener == null) {
			return null;
		}
		return (thread, level, message) -> listener.notifyProgress(level, message);
	}

	/**
	 * empty progress listener
	 *
	 * @return progress listener
	 */
	static MultiThreadListener ignore() {
		return new MultiThreadListener() {
			@Override
			public void notifyProgress(String thread, float level, String message) {
			}

			@Override
			public MultiThreadListener combine(MultiThreadListener listener) {
				if (listener == null) {
					return this;
				}
				return listener;
			}
		};
	}

	/**
	 * @return progress listener returning to sdtout
	 */
	static MultiThreadListener sout() {
		return ((thread, level, message) -> System.out.println("[" + thread + "] " + level + " - " + message));
	}

	/**
	 * progress listener of a nullable listener
	 *
	 * @param listener listener
	 * @return listener or ignore listener
	 */
	static MultiThreadListener ofNullable(MultiThreadListener listener) {
		return listener == null ? ignore() : listener;
	}

	/**
	 * Send progress notification
	 *
	 * @param thread  thread name
	 * @param level   percent of the task accomplished
	 * @param message Description of the operation
	 */
	void notifyProgress(String thread, float level, String message);

	/**
	 * Send progress notification, should call
	 * {@link #notifyProgress(String, float, String)}
	 *
	 * @param level   percent of the task accomplished
	 * @param message Description of the operation
	 */
	default void notifyProgress(float level, String message) {
		notifyProgress(Thread.currentThread().getName(), level, message);
	}

	/**
	 * unregister all the thread
	 */
	default void unregisterAllThreads() {
		// should be filled by implementation if required
	}

	/**
	 * register a thread
	 *
	 * @param threadName the thread name
	 */
	default void registerThread(String threadName) {
		// should be filled by implementation if required
	}

	/**
	 * unregister a thread
	 *
	 * @param threadName the thread name
	 */
	default void unregisterThread(String threadName) {
		// should be filled by implementation if required
	}

	/**
	 * combine a listener with another one into a new listener
	 *
	 * @param listener the listener
	 * @return new listener
	 */
	default MultiThreadListener combine(MultiThreadListener listener) {
		if (listener == null) {
			return this;
		}
		return ((thread, level, message) -> {
			MultiThreadListener.this.notifyProgress(thread, level, message);
			listener.notifyProgress(thread, level, message);
		});
	}

	@Override
	default ProgressListener combine(ProgressListener listener) {
		return combine(ofSingle(listener));
	}
}

package com.the_qa_company.qendpoint.core.util.concurrent;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ProgressListener} wrapper to allow multiple thread to notify a
 * progression
 *
 * @author Antoine Willerval
 */
public class SyncListener implements ProgressListener {
	/**
	 * create a sync listener from another progress listener
	 *
	 * @param listener listener to sync, if it is null, this method returns null
	 * @return sync version of listener, or null if listener is null
	 */
	public static ProgressListener of(ProgressListener listener) {
		return listener instanceof SyncListener || listener == null ? listener : new SyncListener(listener);
	}

	private final ProgressListener wrapper;
	private final ConcurrentLinkedQueue<ProgressUpdate> queue = new ConcurrentLinkedQueue<>();
	private final AtomicBoolean draining = new AtomicBoolean(false);

	private static final class ProgressUpdate {
		private final float level;
		private final String message;

		private ProgressUpdate(float level, String message) {
			this.level = level;
			this.message = message;
		}
	}

	private SyncListener(ProgressListener wrapper) {
		this.wrapper = wrapper;
	}

	@Override
	public void notifyProgress(float level, String message) {
		queue.add(new ProgressUpdate(level, message));
		drainQueue();
	}

	private void drainQueue() {
		if (!draining.compareAndSet(false, true)) {
			return;
		}
		while (true) {
			try {
				ProgressUpdate update;
				while ((update = queue.poll()) != null) {
					wrapper.notifyProgress(update.level, update.message);
				}
			} finally {
				draining.set(false);
			}
			if (queue.isEmpty() || !draining.compareAndSet(false, true)) {
				return;
			}
		}
	}
}

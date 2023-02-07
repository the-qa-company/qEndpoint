package com.the_qa_company.qendpoint.core.util.concurrent;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;

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

	private SyncListener(ProgressListener wrapper) {
		this.wrapper = wrapper;
	}

	@Override
	public synchronized void notifyProgress(float level, String message) {
		wrapper.notifyProgress(level, message);
	}
}

package com.the_qa_company.qendpoint.core.util.listener;

import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;

/**
 * Simple implementation of {@link MultiThreadListener} redirecting all the
 * progression to a progression listener with a prefix
 *
 * @author Antoine Willerval
 */
public class PrefixMultiThreadListener implements MultiThreadListener {

	private final ProgressListener progressListener;

	public PrefixMultiThreadListener(ProgressListener progressListener) {
		this.progressListener = progressListener;
	}

	@Override
	public void notifyProgress(String thread, float level, String message) {
		progressListener.notifyProgress(level, "[" + thread + "]" + message);
	}
}

package com.the_qa_company.qendpoint.core.util.concurrent;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;

import java.io.Closeable;
import java.io.IOException;

/**
 * Class to synchronize a {@link DynamicSequence} map
 *
 * @author Antoine Willerval
 */
public class SyncSeq implements Closeable {
	private final DynamicSequence seq;

	public SyncSeq(DynamicSequence seq) {
		this.seq = seq;
	}

	public synchronized long get(long index) {
		return seq.get(index);
	}

	public synchronized void set(long index, long value) {
		seq.set(index, value);
	}

	@Override
	public synchronized void close() throws IOException {
		seq.close();
	}
}

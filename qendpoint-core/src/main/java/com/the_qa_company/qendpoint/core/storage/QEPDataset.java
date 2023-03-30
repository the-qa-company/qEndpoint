package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.hdt.HDT;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dataset information
 *
 * @param id      string id of the dataset
 * @param path    path of the dataset
 * @param dataset loaded dataset
 * @param uid     uid of the dataset for internal use
 */
record QEPDataset(String id, Path path, HDT dataset, long uid) implements Closeable {
	private static final AtomicLong DATASET_UID_FETCHER = new AtomicLong();

	public QEPDataset(String id, Path path, HDT dataset) {
		this(id, path, dataset, DATASET_UID_FETCHER.incrementAndGet());
	}

	@Override
	public void close() throws IOException {
		dataset.close();
	}
}
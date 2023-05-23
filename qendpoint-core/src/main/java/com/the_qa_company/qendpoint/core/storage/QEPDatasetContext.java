package com.the_qa_company.qendpoint.core.storage;

public interface QEPDatasetContext extends AutoCloseable {
	/**
	 * @return the dataset
	 */
	QEPDataset dataset();

	/**
	 * test if a triple is deleted
	 *
	 * @param tripleID triple id
	 * @return if the triple is deleted
	 */
	boolean isTripleDeleted(long tripleID);

	@Override
	void close();
}

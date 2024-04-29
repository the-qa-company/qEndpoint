package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.storage.iterator.AutoCloseableGeneric;

/**
 * {@link QEPDataset} search context, if a triple is deleted/added after the
 * context creation, it won't be considered.
 *
 * @author Antoine Willerval
 */
public interface QEPDatasetContext extends AutoCloseableGeneric<QEPCoreException> {
	/**
	 * @return the dataset
	 */
	QEPDataset dataset();

	/**
	 * @return the delete bitmap linked with the context, it doesn't consider
	 *         the triples deleted after the context creation
	 */
	Bitmap deleteBitmap();

	/**
	 * test if a triple is deleted
	 *
	 * @param tripleID triple id
	 * @return if the triple is deleted
	 */
	boolean isTripleDeleted(long tripleID);

	@Override
	void close();

	/**
	 * @return if the context has a deletion
	 */
	boolean hasDeletions();
}

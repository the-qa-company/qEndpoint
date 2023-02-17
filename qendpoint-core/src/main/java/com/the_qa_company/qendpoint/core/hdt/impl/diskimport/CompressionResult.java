package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;

import java.io.Closeable;
import java.io.IOException;

/**
 * Result for the {@link SectionCompressor}
 *
 * @author Antoine Willerval
 */
public interface CompressionResult extends Closeable {
	/**
	 * partial mode for config
	 *
	 * @see CompressionResultPartial
	 */
	String COMPRESSION_MODE_PARTIAL = HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_PARTIAL;
	/**
	 * complete mode for config
	 *
	 * @see CompressionResultFile
	 */
	String COMPRESSION_MODE_COMPLETE = HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE;

	/**
	 * @return the number of triple
	 */
	long getTripleCount();

	/**
	 * @return a sorted iterator of subject
	 */
	ExceptionIterator<IndexedNode, IOException> getSubjects();

	/**
	 * @return a sorted iterator of predicates
	 */
	ExceptionIterator<IndexedNode, IOException> getPredicates();

	/**
	 * @return a sorted iterator of objects
	 */
	ExceptionIterator<IndexedNode, IOException> getObjects();

	/**
	 * @return the count of subjects
	 */
	long getSubjectsCount();

	/**
	 * @return the count of predicates
	 */
	long getPredicatesCount();

	/**
	 * @return the count of objects
	 */
	long getObjectsCount();

	/**
	 * @return the count of shared
	 */
	long getSharedCount();

	/**
	 * @return the size of the origin file
	 */
	long getRawSize();

	/**
	 * delete data associated with this result
	 */
	void delete() throws IOException;
}

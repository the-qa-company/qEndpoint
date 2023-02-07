package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.util.io.compress.MapCompressTripleMerger;

import java.io.Closeable;

/**
 * Result for the {@link MapCompressTripleMerger}
 *
 * @author Antoine Willerval
 */
public interface TripleCompressionResult extends Closeable {
	/**
	 * @return a sorted iterator of subject
	 */
	TempTriples getTriples();

	/**
	 * @return the number of triples
	 */
	long getTripleCount();
}

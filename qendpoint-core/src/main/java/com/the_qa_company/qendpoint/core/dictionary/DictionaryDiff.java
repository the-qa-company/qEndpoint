package com.the_qa_company.qendpoint.core.dictionary;

import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatMapping;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface DictionaryDiff extends Closeable {
	/**
	 * compute the diff of the previous dictionary
	 *
	 * @param dictionary previous dictionary
	 * @param bitmaps    the bitmap for each sections
	 * @param listener   listener to get the progress
	 * @throws IOException io error
	 */
	void diff(Dictionary dictionary, Map<CharSequence, ModifiableBitmap> bitmaps, ProgressListener listener)
			throws IOException;

	/**
	 * @return the CatMapping of the diff
	 */
	CatMapping getMappingBack();

	/**
	 * @return the new number of shared element
	 */
	long getNumShared();

	/**
	 * @return the cat mapping for each section
	 */
	Map<ByteString, CatMapping> getAllMappings();
}

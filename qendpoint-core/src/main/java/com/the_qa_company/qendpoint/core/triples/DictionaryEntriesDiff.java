package com.the_qa_company.qendpoint.core.triples;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.triples.impl.FourSectionDictionaryEntriesDiff;
import com.the_qa_company.qendpoint.core.triples.impl.MultipleSectionDictionaryEntriesDiff;

import java.util.Map;

/**
 * Class to compute for each dictionary section a bitmap indicating if the
 * corresponding entry needs to be deleted or not
 */
public interface DictionaryEntriesDiff {
	/**
	 * create a {@link DictionaryEntriesDiff} for a particular dictionary type
	 *
	 * @param dictionary   the dictionary to get the type
	 * @param hdt          argument to create the Iterator
	 * @param deleteBitmap argument to create the Iterator
	 * @param hdtIterator  argument to create the Iterator
	 * @return iterator
	 */
	static DictionaryEntriesDiff createForType(Dictionary dictionary, HDT hdt, Bitmap deleteBitmap,
			IteratorTripleID hdtIterator) {
		String type = dictionary.getType();

		// four section dictionaries
		if (HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION.equals(type)
				|| type.equals(HDTVocabulary.DICTIONARY_TYPE_FOUR_PSFC_SECTION))
			return new FourSectionDictionaryEntriesDiff(hdt, deleteBitmap, hdtIterator);

		// multiple section dictionaries
		if (type.equals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION))
			return new MultipleSectionDictionaryEntriesDiff(hdt, deleteBitmap, hdtIterator);

		throw new IllegalFormatException("Implementation of BitmapTriplesIteratorDiffBit not found for " + type);
	}

	/**
	 * @return the bitmaps
	 */
	Map<CharSequence, ModifiableBitmap> getBitmaps();

	/**
	 * create the bitmaps
	 */
	void loadBitmaps();

	/**
	 * @return the count
	 */
	long getCount();
}

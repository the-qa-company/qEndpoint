package com.the_qa_company.qendpoint.core.search;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;

import java.util.Objects;

/**
 * Implementation of {@link HDTQueryToolFactory}
 *
 * @author Antoine Willerval
 */
public class HDTQueryToolFactoryImpl extends HDTQueryToolFactory {

	@Override
	public HDTQueryTool newGenericQueryTool(HDT hdt) {
		Objects.requireNonNull(hdt, "hdt can't be null!");
		return new SimpleQueryTool(hdt);
	}

	@Override
	public boolean hasGenericTool() {
		return true;
	}

	@Override
	public HDTQueryTool newQueryTool(HDT hdt) {
		// check both the type and the getType() to be sure no one is reusing
		// the code (extends) or
		// implementing his own version (getType)
		if (HDTVocabulary.BITMAP_TYPE_PLAIN.equals(hdt.getTriples().getType())) {
			if (hdt.getTriples() instanceof BitmapTriples) {
				return new BitmapTriplesQueryTool(hdt);
			}
		}
		return null;
	}
}

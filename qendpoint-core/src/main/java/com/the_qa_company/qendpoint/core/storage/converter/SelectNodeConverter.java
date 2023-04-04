package com.the_qa_company.qendpoint.core.storage.converter;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;

/**
 * Converter using the select mapping
 *
 * @author Antoine Willerval
 */
public class SelectNodeConverter implements NodeConverter {
	private final Bitmap bitmap;
	private final Sequence sequence;

	public SelectNodeConverter(Bitmap bitmap, Sequence sequence) {
		this.bitmap = bitmap;
		this.sequence = sequence;
	}

	@Override
	public long mapValue(long id) {
		if (id <= 0 || bitmap.access(id)) {
			return 0;
		}
		return sequence.get(bitmap.rank1(id));
	}
}

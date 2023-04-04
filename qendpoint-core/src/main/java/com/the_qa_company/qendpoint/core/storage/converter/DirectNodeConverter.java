package com.the_qa_company.qendpoint.core.storage.converter;

import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;

/**
 * Converter using the direct mapping
 *
 * @author Antoine Willerval
 */
public class DirectNodeConverter implements NodeConverter {
	private final Sequence map;

	public DirectNodeConverter(Sequence map) {
		this.map = map;
	}

	@Override
	public long mapValue(long id) {
		if (id <= 0 || id > map.getNumberOfElements()) {
			return 0;
		}
		return map.get(id);
	}
}

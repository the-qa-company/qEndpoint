package com.the_qa_company.qendpoint.core.storage.converter;

/**
 * Converter switching the mapping to a subject or an object converter depending
 * on the id
 *
 * @param sharedCount      shared count in the origin dataset
 * @param subjectConverter subject converter
 * @param objectConverter  object converter
 * @author Antoine Willerval
 */
public record SharedWrapperNodeConverter(long sharedCount, NodeConverter subjectConverter,
		NodeConverter objectConverter) implements NodeConverter {

	@Override
	public long mapValue(long id) {
		if (id > sharedCount) {
			// using the object converter to fetch the id
			return objectConverter.mapValue(id - sharedCount);
		}

		if (id <= 0) {
			// oob
			return 0;
		}
		// using the subject converter to fetch the id
		return subjectConverter.mapValue(id);
	}

	@Override
	public String toString() {
		return String.format("SharedWrapperNodeConverter[shared=%X]", sharedCount);
	}
}

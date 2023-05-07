package com.the_qa_company.qendpoint.core.storage.converter;

/**
 * Converter switching the mapping to a subject or an object converter depending
 * on the id
 *
 * @author Antoine Willerval
 */
public class SharedWrapperNodeConverter implements NodeConverter {
	private final long sharedCount;
	private final NodeConverter subjectConverter;
	private final NodeConverter objectConverter;

	/**
	 * converter
	 *
	 * @param sharedCount      shared count in the origin dataset
	 * @param subjectConverter subject converter
	 * @param objectConverter  object converter
	 */
	public SharedWrapperNodeConverter(long sharedCount, NodeConverter subjectConverter, NodeConverter objectConverter) {
		this.sharedCount = sharedCount;
		this.subjectConverter = subjectConverter;
		this.objectConverter = objectConverter;
	}

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
}

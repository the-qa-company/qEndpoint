package com.the_qa_company.qendpoint.core.storage.converter;

/**
 * Converter to map ids
 *
 * @author Antoine Willerval
 */
public interface NodeConverter {
	/**
	 * map the node id
	 *
	 * @param id node id
	 * @return mapped id if available, 0 for not found ids
	 */
	long mapValue(long id);
}

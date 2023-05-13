package com.the_qa_company.qendpoint.core.storage.converter;

import com.the_qa_company.qendpoint.core.util.disk.LongArray;

/**
 * Converter using the id/map permutation map
 *
 * @author Antoine Willerval
 */
public record PermutationNodeConverter(LongArray idSequence, LongArray mapSequence) implements NodeConverter {

	@Override
	public long mapValue(long id) {
		long permutationId = idSequence.binarySearch(id, 1, idSequence.length());
		if (permutationId == -1) {
			return 0;
		}
		return mapSequence.get(permutationId);
	}
}

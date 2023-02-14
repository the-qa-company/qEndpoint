package com.the_qa_company.qendpoint.core.search.optimizer;

import com.the_qa_company.qendpoint.core.search.component.HDTComponentTriple;

import java.util.List;

/**
 * Tool to optimize queries
 *
 * @author Antoine Willerval
 */
@FunctionalInterface
public interface Optimizer {
	/**
	 * optimize a list of patterns
	 *
	 * @param patterns the patterns
	 */
	void optimize(List<HDTComponentTriple> patterns);
}

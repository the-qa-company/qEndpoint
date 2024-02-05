package com.the_qa_company.qendpoint.store;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

public class EndpointStoreEvaluationStatistics extends EvaluationStatistics {

	private final EndpointStore store;
	private final EndpointStoreEvaluationStatisticsHDT hdtStats;
	private final CardinalityCalculator calculator;
	public EndpointStoreEvaluationStatistics(EndpointStore store) {
		// new EndpointStoreEvaluationStatisticsHDT(endpoint), endpoint.getCurrentSaliStore().getEvaluationStatistics()
		this.store = store;
		this.hdtStats = new EndpointStoreEvaluationStatisticsHDT(store);
		this.calculator = new CombinedCardinalityCalculator();
	}

	@Override
	protected CardinalityCalculator createCardinalityCalculator() {
		return calculator;
	}

	private class CombinedCardinalityCalculator extends CardinalityCalculator {
		@Override
		protected double getCardinality(StatementPattern sp) {
			double hdtCard = hdtStats.getCardinality(sp);
			double nativeCard = store.getCurrentSaliStore().getEvaluationStatistics().getCardinality(sp);
			if ((long)hdtCard == Integer.MAX_VALUE && nativeCard > 0) {
				hdtCard = 0;
			}
			double cardinality = hdtCard + nativeCard;
			System.out.println("Cardinality for " + sp.getSubjectVar() + ", " + sp.getPredicateVar() + ", " + sp.getObjectVar()
			                   + " : " + cardinality + " (HDT: " + hdtCard + ", Native: " + nativeCard + ")\n");
			return cardinality;
		}
	}

}

package com.the_qa_company.qendpoint.store;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

public class EndpointStoreEvaluationStatistics extends EvaluationStatistics {

	private final EndpointStoreEvaluationStatisticsHDT endpointStoreEvaluationStatisticsHDT;
	private final EvaluationStatistics nativeEvaluationStatistics;

	public EndpointStoreEvaluationStatistics(EndpointStoreEvaluationStatisticsHDT endpointStoreEvaluationStatisticsHDT,
			EvaluationStatistics nativeEvaluationStatistics) {
		this.endpointStoreEvaluationStatisticsHDT = endpointStoreEvaluationStatisticsHDT;
		this.nativeEvaluationStatistics = nativeEvaluationStatistics;
	}

	@Override
	protected CardinalityCalculator createCardinalityCalculator() {
		return new CombinedCardinalityCalculator();
	}

	private class CombinedCardinalityCalculator extends CardinalityCalculator {
		@Override
		protected double getCardinality(StatementPattern sp) {
			double hdtCard = endpointStoreEvaluationStatisticsHDT.getCardinality(sp);
			double nativeCard = nativeEvaluationStatistics.getCardinality(sp);
			if (hdtCard == Integer.MAX_VALUE && nativeCard > 0) {
				hdtCard = 0;
			}
			double cardinality = hdtCard + nativeCard;
			System.out.println("Cardinality for " + sp.toString().replace("\n", " ") + " is " + cardinality + " (HDT: " + hdtCard + ", Native: " + nativeCard + ")\n");
			return cardinality;
		}
	}

}

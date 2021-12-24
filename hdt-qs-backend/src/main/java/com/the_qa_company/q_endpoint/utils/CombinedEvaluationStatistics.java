package com.the_qa_company.q_endpoint.utils;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

public class CombinedEvaluationStatistics extends EvaluationStatistics {

    private HDTEvaluationStatistics hdtEvaluationStatistics;
    private EvaluationStatistics nativeEvaluationStatistics;

    public CombinedEvaluationStatistics(HDTEvaluationStatistics hdtEvaluationStatistics,
                                        EvaluationStatistics nativeEvaluationStatistics) {
        this.hdtEvaluationStatistics = hdtEvaluationStatistics;
        this.nativeEvaluationStatistics = nativeEvaluationStatistics;
    }

    @Override
    protected CardinalityCalculator createCardinalityCalculator() {
        return new CombinedCardinalityCalculator();
    }

    private class CombinedCardinalityCalculator extends CardinalityCalculator {
        @Override
        protected double getCardinality(StatementPattern sp) {
            double hdtCard = hdtEvaluationStatistics.getCardinality(sp);
            double nativeCard = nativeEvaluationStatistics.getCardinality(sp);
            if (hdtCard == Integer.MAX_VALUE && nativeCard > 0)
                hdtCard = 0;
            return hdtCard + nativeCard;
        }
    }

}

package org.rdfhdt.hdt.rdf4j;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

public class CombinedEvaluationStatistics extends EvaluationStatistics {

    private HDTEvaluationStatisticsV2 hdtEvaluationStatistics;
    private EvaluationStatistics nativeEvaluationStatistics;

    public CombinedEvaluationStatistics(HDTEvaluationStatisticsV2 hdtEvaluationStatistics,
                                        EvaluationStatistics nativeEvaluationStatistics){
    this.hdtEvaluationStatistics = hdtEvaluationStatistics;
    this.nativeEvaluationStatistics = nativeEvaluationStatistics;
    }

    @Override
    public synchronized double getCardinality(TupleExpr expr) {
        double hdtCard = hdtEvaluationStatistics.getCardinality(expr);
        double nativeCard = nativeEvaluationStatistics.getCardinality(expr);
        System.out.println(" HDT stat: ---------------------- "+hdtCard);
        System.out.println(" Native stat: ---------------------- "+nativeCard);
        if(hdtCard == Integer.MAX_VALUE && nativeCard >0)
            hdtCard = 0;
        return hdtCard + nativeCard;
    }
}

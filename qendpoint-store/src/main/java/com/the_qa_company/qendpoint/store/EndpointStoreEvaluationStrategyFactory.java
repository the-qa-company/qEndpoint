package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.model.HDTCompareOp;
import org.eclipse.rdf4j.common.transaction.QueryEvaluationMode;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryValueEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategyFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtil;

public class EndpointStoreEvaluationStrategyFactory extends DefaultEvaluationStrategyFactory {
	private final EndpointStore store;

	public EndpointStoreEvaluationStrategyFactory(EndpointStore store) {
		this.store = store;
	}

	public EndpointStoreEvaluationStrategyFactory(FederatedServiceResolver resolver, EndpointStore store) {
		super(resolver);
		this.store = store;
	}

	@Override
	public EvaluationStrategy createEvaluationStrategy(Dataset dataset, TripleSource tripleSource,
			EvaluationStatistics evaluationStatistics) {
		EvaluationStrategy strategy = new EndpointStoreEvaluationStrategy(tripleSource, dataset,
				getFederatedServiceResolver(), getQuerySolutionCacheThreshold(), evaluationStatistics,
				isTrackResultSize());
		getOptimizerPipeline().ifPresent(strategy::setOptimizerPipeline);
		strategy.setCollectionFactory(store.getCollectionFactory());
		return strategy;
	}

	public class EndpointStoreEvaluationStrategy extends DefaultEvaluationStrategy {
		public EndpointStoreEvaluationStrategy(TripleSource tripleSource, Dataset dataset,
				FederatedServiceResolver serviceResolver, long iterationCacheSyncTreshold,
				EvaluationStatistics evaluationStatistics, boolean trackResultSize) {
			super(tripleSource, dataset, serviceResolver, iterationCacheSyncTreshold, evaluationStatistics,
					trackResultSize);
		}

		@Override
		public QueryValueEvaluationStep precompile(ValueExpr expr, QueryEvaluationContext context)
				throws QueryEvaluationException {
			if (expr instanceof HDTCompareOp hcop) {
				HDT hdt = store.getHdt();

				boolean strict = QueryEvaluationMode.STRICT == getQueryEvaluationMode();
				return supplyBinaryValueEvaluation(hcop, (Value leftVal, Value rightVal) -> {
					System.out.println("compare: " + leftVal + "(" + leftVal.getClass().getSimpleName() + ")" + "/"
							+ rightVal + "(" + rightVal.getClass().getSimpleName() + ")");
					return BooleanLiteral.valueOf(QueryEvaluationUtil.compare(leftVal, rightVal, hcop.getOp(), strict));
				}, context);
			}
			return super.precompile(expr, context);
		}
	}
}

package com.the_qa_company.qendpoint.store;

import org.eclipse.rdf4j.collection.factory.api.CollectionFactory;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategyFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

import java.util.function.Supplier;

public class CustomEvaluationStrategyFactory extends DefaultEvaluationStrategyFactory {

	private Supplier<CollectionFactory> collectionFactorySupplier;

	public CustomEvaluationStrategyFactory() {
		super();
	}

	public CustomEvaluationStrategyFactory(FederatedServiceResolver resolver) {
		super(resolver);
	}

	@Override
	public void setCollectionFactory(Supplier<CollectionFactory> collectionFactory) {
		this.collectionFactorySupplier = collectionFactory;
	}

	@Override
	public EvaluationStrategy createEvaluationStrategy(Dataset dataset, TripleSource tripleSource,
			EvaluationStatistics evaluationStatistics) {
		var strategy = new CustomEvaluationStrategy(tripleSource, dataset, getFederatedServiceResolver(),
				getQuerySolutionCacheThreshold(), evaluationStatistics, isTrackResultSize());
		getOptimizerPipeline().ifPresent(strategy::setOptimizerPipeline);
		strategy.setCollectionFactory(collectionFactorySupplier);
		return strategy;
	}
}

package com.the_qa_company.qendpoint.utils.sail;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingAssigner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.FilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.TupleFunctionEvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.TupleFunctionEvaluationStrategy;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.evaluation.SailTripleSource;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;

import java.util.function.Supplier;

public class OptimizingSail extends NotifyingSailWrapper {
	private final Supplier<FederatedServiceResolver> federatedServiceResolverSupplier;

	public OptimizingSail(NotifyingSail baseSail, Supplier<FederatedServiceResolver> federatedServiceResolverSupplier) {
		super(baseSail);
		this.federatedServiceResolverSupplier = federatedServiceResolverSupplier;
	}

	@Override
	public NotifyingSailConnection getConnection() throws SailException {
		return new OptimizingSailConnection(super.getConnection());
	}

	private class OptimizingSailConnection extends NotifyingSailConnectionWrapper {
		public OptimizingSailConnection(NotifyingSailConnection wrappedCon) {
			super(wrappedCon);
		}

		@Override
		public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr,
				Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
			ValueFactory vf = getValueFactory();
			EvaluationStrategy strategy = new TupleFunctionEvaluationStrategy(
					new SailTripleSource(this, includeInferred, vf), dataset, federatedServiceResolverSupplier.get());
			(new BindingAssigner()).optimize(tupleExpr, dataset, bindings);
			(new ConstantOptimizer(strategy)).optimize(tupleExpr, dataset, bindings);
			(new CompareOptimizer()).optimize(tupleExpr, dataset, bindings);
			(new ConjunctiveConstraintSplitter()).optimize(tupleExpr, dataset, bindings);
			(new DisjunctiveConstraintOptimizer()).optimize(tupleExpr, dataset, bindings);
			(new SameTermFilterOptimizer()).optimize(tupleExpr, dataset, bindings);
			(new QueryModelNormalizer()).optimize(tupleExpr, dataset, bindings);
			(new QueryJoinOptimizer(new TupleFunctionEvaluationStatistics())).optimize(tupleExpr, dataset, bindings);
			(new IterativeEvaluationOptimizer()).optimize(tupleExpr, dataset, bindings);
			// FIXME: remove comment
			// (new FilterOptimizer()).optimize(tupleExpr, dataset, bindings);
			(new OrderLimitOptimizer()).optimize(tupleExpr, dataset, bindings);

			try {
				return strategy.evaluate(tupleExpr, bindings);
			} catch (QueryEvaluationException e) {
				throw new SailException(e);
			}
		}
	}
}

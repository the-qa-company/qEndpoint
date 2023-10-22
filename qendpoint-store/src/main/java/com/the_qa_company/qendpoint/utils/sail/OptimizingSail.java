package com.the_qa_company.qendpoint.utils.sail;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.BindingAssignerOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ConjunctiveConstraintSplitterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.QueryModelNormalizerOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.SameTermFilterOptimizer;
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
        public CloseableIteration<? extends BindingSet> evaluate(TupleExpr tupleExpr,
                                                                 Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
            ValueFactory vf = getValueFactory();
            EvaluationStrategy strategy = new TupleFunctionEvaluationStrategy(
                    new SailTripleSource(this, includeInferred, vf), dataset, federatedServiceResolverSupplier.get());
            (new BindingAssignerOptimizer()).optimize(tupleExpr, dataset, bindings);
            (new ConstantOptimizer(strategy)).optimize(tupleExpr, dataset, bindings);
            (new CompareOptimizer()).optimize(tupleExpr, dataset, bindings);
            (new ConjunctiveConstraintSplitterOptimizer()).optimize(tupleExpr, dataset, bindings);
            (new DisjunctiveConstraintOptimizer()).optimize(tupleExpr, dataset, bindings);
            (new SameTermFilterOptimizer()).optimize(tupleExpr, dataset, bindings);
            (new QueryModelNormalizerOptimizer()).optimize(tupleExpr, dataset, bindings);
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

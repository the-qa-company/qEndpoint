package eu.qanswer.misc;

import eu.qanswer.hybridstore.HybridTripleSource;
import eu.qanswer.utils.HDTEvaluationStatisticsV2;
import eu.qanswer.utils.VariableToIdSubstitution;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.AbstractQueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.*;
import org.eclipse.rdf4j.query.impl.IteratingTupleQueryResult;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.impl.AbstractParserQuery;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;
import org.rdfhdt.hdt.hdt.HDT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class HDTQueryPreparer extends AbstractQueryPreparer {
  private static final Logger LOGGER = LoggerFactory.getLogger("queryPreparer");

  HDT hdt;
  // FIX ME for joint optimization
  private EvaluationStatistics evaluationStatistics;

  public HDTQueryPreparer(HybridTripleSource tripleSource) {
    super(tripleSource);
    hdt = tripleSource.getHdt();
    // @Ali check here
    evaluationStatistics = new HDTEvaluationStatisticsV2(hdt);
    // evaluationStatistics = new EvaluationStatistics();
  }

  @Override
  protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
      TupleExpr tupleExpr,
      Dataset dataset,
      BindingSet bindings,
      boolean includeInferred,
      int maxExecutionTime)
      throws QueryEvaluationException {
    if (!(tupleExpr instanceof QueryRoot)) {
      tupleExpr = new QueryRoot(tupleExpr);
    }

    EvaluationStrategy strategy =
        new ExtendedEvaluationStrategy(
            getTripleSource(), dataset, new SPARQLServiceResolver(), 0L, evaluationStatistics);

    new VariableToIdSubstitution(hdt).optimize(tupleExpr, dataset, bindings);
    new BindingAssigner().optimize(tupleExpr, dataset, bindings);
    new ConstantOptimizer(strategy).optimize(tupleExpr, dataset, bindings);
    new CompareOptimizer().optimize(tupleExpr, dataset, bindings);
    new ConjunctiveConstraintSplitter().optimize(tupleExpr, dataset, bindings);
    new DisjunctiveConstraintOptimizer().optimize(tupleExpr, dataset, bindings);
    new SameTermFilterOptimizer().optimize(tupleExpr, dataset, bindings);
    new QueryModelNormalizer().optimize(tupleExpr, dataset, bindings);
    new QueryJoinOptimizer(evaluationStatistics).optimize(tupleExpr, dataset, bindings);
    new IterativeEvaluationOptimizer().optimize(tupleExpr, dataset, bindings);
    // new FilterOptimizer().optimize(tupleExpr, dataset, bindings);
    new OrderLimitOptimizer().optimize(tupleExpr, dataset, bindings);

    return strategy.evaluate(tupleExpr, bindings);
  }

  @Override
  protected void execute(
      UpdateExpr updateExpr,
      Dataset dataset,
      BindingSet bindings,
      boolean includeInferred,
      int maxExecutionTime)
      throws UpdateExecutionException {
    throw new UpdateExecutionException("This repository is read only");
  }
  ////////////////////////////////////
  // this fucntion must be implemented here ..
  @Override
  public TupleQuery prepare(ParsedTupleQuery q) {
    return new HDTTupleQueryImpl(q);
  }
  ////////////////////////////////
  class HDTTupleQueryImpl extends AbstractParserQuery implements TupleQuery {
    HDTTupleQueryImpl(ParsedTupleQuery query) {
      super(query);
    }

    public ParsedTupleQuery getParsedQuery() {
      return (ParsedTupleQuery) super.getParsedQuery();
    }

    public TupleQueryResult evaluate() throws QueryEvaluationException {
      CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter1 = null;
      CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter2 = null;
      IteratingTupleQueryResult result = null;
      boolean allGood = false;

      IteratingTupleQueryResult var6;
      try {
        TupleExpr tupleExpr = this.getParsedQuery().getTupleExpr();
        bindingsIter1 =
            HDTQueryPreparer.this.evaluate(
                tupleExpr,
                this.getActiveDataset(),
                this.getBindings(),
                this.getIncludeInferred(),
                this.getMaxExecutionTime());
        bindingsIter2 = this.enforceMaxQueryTime(bindingsIter1);
        result =
            new IteratingTupleQueryResult(
                new ArrayList(tupleExpr.getBindingNames()), bindingsIter2);
        allGood = true;
        var6 = result;
      } finally {
        if (!allGood) {
          try {
            if (result != null) {
              result.close();
            }
          } finally {
            try {
              if (bindingsIter2 != null) {
                bindingsIter2.close();
              }
            } finally {
              if (bindingsIter1 != null) {
                bindingsIter1.close();
              }
            }
          }
        }
      }

      return var6;
    }

    public void evaluate(TupleQueryResultHandler handler)
        throws QueryEvaluationException, TupleQueryResultHandlerException {
      TupleQueryResult queryResult = this.evaluate();
      QueryResults.report(queryResult, handler);
    }
  }
}

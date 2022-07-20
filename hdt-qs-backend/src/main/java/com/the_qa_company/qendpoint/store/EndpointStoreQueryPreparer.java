package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.federation.SPARQLServiceWikibaseLabelResolver;
import com.the_qa_company.qendpoint.federation.ServiceClauseOptimizer;
import com.the_qa_company.qendpoint.utils.VariableToIdSubstitution;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.AbstractQueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExtendedEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.BindingAssignerOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ConjunctiveConstraintSplitterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.FilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.QueryModelNormalizerOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.explanation.Explanation;
import org.eclipse.rdf4j.query.impl.IteratingTupleQueryResult;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.impl.AbstractParserQuery;

import java.util.ArrayList;

// @todo: are there any changes in this class except that the evaluationStatistics is using the CombinedEvaluationStatistics?
// yes
public class EndpointStoreQueryPreparer extends AbstractQueryPreparer {

	private final EvaluationStatistics evaluationStatistics;
	private final EndpointTripleSource tripleSource;
	private final EndpointStore endpoint;
	private boolean trackResultSize;
	private boolean cloneTupleExpression;
	private boolean trackTime;

	public EndpointStoreQueryPreparer(EndpointStore endpoint, EndpointTripleSource tripleSource) {
		super(tripleSource);
		this.tripleSource = tripleSource;
		this.endpoint = endpoint;
		cloneTupleExpression = true;

		evaluationStatistics = new EndpointStoreEvaluationStatistics(new EndpointStoreEvaluationStatisticsHDT(endpoint),
				endpoint.getCurrentSaliStore().getEvaluationStatistics());
	}

	public void setExplanationLevel(Explanation.Level level) {
		if (level == null) {
			this.cloneTupleExpression = true;
			this.trackResultSize = false;
			this.trackTime = false;
			return;
		}

		switch (level) {
		case Timed:
			this.trackTime = true;
			this.trackResultSize = true;
			this.cloneTupleExpression = false;
			break;
		case Executed:
			this.trackResultSize = true;
			this.cloneTupleExpression = false;
			break;
		case Optimized:
			this.cloneTupleExpression = false;
		case Unoptimized:
			break;
		default:
			throw new UnsupportedOperationException("Unsupported query explanation level: " + level);
		}
	}

	@Override
	public EndpointTripleSource getTripleSource() {
		return tripleSource;
	}

	@Override
	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr,
			Dataset dataset, BindingSet bindings, boolean includeInferred, int maxExecutionTime)
			throws QueryEvaluationException {

		if (this.cloneTupleExpression) {
			tupleExpr = tupleExpr.clone();
		}
		if (!(tupleExpr instanceof QueryRoot)) {
			tupleExpr = new QueryRoot(tupleExpr);
		}
		EvaluationStrategy strategy = new ExtendedEvaluationStrategy(getTripleSource(), dataset,
				new SPARQLServiceWikibaseLabelResolver(tripleSource), 0L, evaluationStatistics);

		if (this.trackResultSize) {
			strategy.setTrackResultSize(this.trackResultSize);
		}

		if (this.trackTime) {
			strategy.setTrackTime(this.trackTime);
		}

		new VariableToIdSubstitution(endpoint).optimize(tupleExpr, dataset, bindings);
		new BindingAssignerOptimizer().optimize(tupleExpr, dataset, bindings);
		new ConstantOptimizer(strategy).optimize(tupleExpr, dataset, bindings);
		new CompareOptimizer().optimize(tupleExpr, dataset, bindings);
		new ConjunctiveConstraintSplitterOptimizer().optimize(tupleExpr, dataset, bindings);
		new DisjunctiveConstraintOptimizer().optimize(tupleExpr, dataset, bindings);
		new SameTermFilterOptimizer().optimize(tupleExpr, dataset, bindings);
		new QueryModelNormalizerOptimizer().optimize(tupleExpr, dataset, bindings);
		new QueryJoinOptimizer(evaluationStatistics).optimize(tupleExpr, dataset, bindings);
		new IterativeEvaluationOptimizer().optimize(tupleExpr, dataset, bindings);
		new FilterOptimizer().optimize(tupleExpr, dataset, bindings);
		new OrderLimitOptimizer().optimize(tupleExpr, dataset, bindings);
		new ServiceClauseOptimizer().optimize(tupleExpr, dataset, bindings);

		return strategy.evaluate(tupleExpr, bindings);
	}

	// @todo: this looks wrong, apperently if one wraps around the store
	// SailRepository then the function is
	// overwritten, this is the reason we do not say an error
	@Override
	protected void execute(UpdateExpr updateExpr, Dataset dataset, BindingSet bindings, boolean includeInferred,
			int maxExecutionTime) throws UpdateExecutionException {
		throw new UpdateExecutionException("This repository is read only");
	}

	@Override
	public TupleQuery prepare(ParsedTupleQuery q) {
		return new HDTTupleQueryImpl(q);
	}

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
			IteratingTupleQueryResult result;
			boolean allGood = false;

			IteratingTupleQueryResult var6;
			try {
				TupleExpr tupleExpr = this.getParsedQuery().getTupleExpr();
				bindingsIter1 = EndpointStoreQueryPreparer.this.evaluate(tupleExpr, this.getActiveDataset(),
						this.getBindings(), this.getIncludeInferred(), this.getMaxExecutionTime());
				bindingsIter2 = this.enforceMaxQueryTime(bindingsIter1);
				result = new IteratingTupleQueryResult(new ArrayList<>(tupleExpr.getBindingNames()), bindingsIter2);
				allGood = true;
				var6 = result;
			} finally {
				if (!allGood) {
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

			return var6;
		}

		public void evaluate(TupleQueryResultHandler handler)
				throws QueryEvaluationException, TupleQueryResultHandlerException {
			TupleQueryResult queryResult = this.evaluate();
			QueryResults.report(queryResult, handler);
		}
	}
}

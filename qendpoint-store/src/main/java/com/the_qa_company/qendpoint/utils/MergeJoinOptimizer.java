package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.EndpointStoreConnection;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.ArrayList;
import java.util.List;

public class MergeJoinOptimizer implements QueryOptimizer {
	private final EndpointStoreConnection conn;

	public MergeJoinOptimizer(EndpointStoreConnection conn) {
		this.conn = conn;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindingSet) {
		if (!conn.getTripleSource().hasEnableMergeJoin()
				|| conn.hasConfig(EndpointStore.QUERY_CONFIG_NO_OPTIMIZER_MERGE)) {
			return; // merge join disabled, ignore
		}

		ModelVisitor visitor = new ModelVisitor();
		tupleExpr.visit(visitor);

	}

	protected static class ModelVisitor extends AbstractQueryModelVisitor<RuntimeException> {

		private boolean getJoinPatterns(Join node, List<StatementPattern> patterns) {
			TupleExpr la = node.getLeftArg();
			if (la instanceof Join laj && getJoinPatterns(laj, patterns)) {
				return true;
			}
			if (!(la instanceof StatementPattern stmt)) {
				return true;
			}
			patterns.add(stmt);

			TupleExpr ra = node.getRightArg();
			if (ra instanceof Join raj && getJoinPatterns(raj, patterns)) {
				return true;
			}
			if (!(ra instanceof StatementPattern stmt2)) {
				return true;
			}
			patterns.add(stmt2);
			return false;
		}

		private List<StatementPattern> getJoinPatterns(Join node) {
			List<StatementPattern> patterns = new ArrayList<>();
			if (getJoinPatterns(node, patterns)) {
				return List.of();
			}
			return patterns;
		}

		@Override
		public void meet(Join node) {
			// stack the triple patterns
			TupleExpr la = node.getLeftArg();
			TupleExpr ra = node.getRightArg();

			List<StatementPattern> patterns = getJoinPatterns(node);

			if (patterns.isEmpty()) {
				super.meet(node);
				return;
			}

			for (StatementPattern p : patterns) {
				// TODO: we can replace the patterns
				p.getObjectVar().hasValue();
			}

		}

		@Override
		public void meet(LeftJoin node) {
			super.meet(node);
		}

	}
}

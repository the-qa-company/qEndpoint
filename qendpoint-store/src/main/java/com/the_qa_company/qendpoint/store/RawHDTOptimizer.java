package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.dictionary.impl.RawDictionary;
import com.the_qa_company.qendpoint.model.HDTCompareOp;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class RawHDTOptimizer implements QueryOptimizer {
	private final RawDictionary rd;

	public RawHDTOptimizer(RawDictionary rd) {
		this.rd = rd;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new RawVisitor());
	}

	private class RawVisitor extends AbstractQueryModelVisitor<RuntimeException> {

		@Override
		public void meet(Compare node) throws RuntimeException {
			HDTCompareOp.replaceIfRequired(node, rd);
			super.meet(node);
		}
	}
}

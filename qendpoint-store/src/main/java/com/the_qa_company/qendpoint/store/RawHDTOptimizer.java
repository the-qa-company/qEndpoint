package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.dictionary.impl.RawDictionary;
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


	}

	private class RawVisitor extends AbstractQueryModelVisitor<RuntimeException> {
		@Override
		public void meet(Var node) throws RuntimeException {
			Value val = node.getValue();
			if (val != null && val.isLiteral()) {
				Literal lit = (Literal) val;

				CoreDatatype cdt = lit.getCoreDatatype();
				if (cdt != null && cdt.isXSDDatatype() && cdt.asXSDDatatype().orElseThrow().isNumericDatatype()) {
					// number, place custom node


				}
			}
			super.meet(node);
		}

		@Override
		public void meet(Compare node) throws RuntimeException {



			super.meet(node);
		}
	}
}

package com.the_qa_company.qendpoint.model;

import com.the_qa_company.qendpoint.core.dictionary.impl.RawDictionary;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.algebra.BinaryValueOperator;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

public class HDTCompareOp extends BinaryValueOperator {
	private static Compare.CompareOp inverse(Compare.CompareOp op) {
		return switch (op) {
		case EQ, NE -> op;
		case LT -> Compare.CompareOp.GE;
		case LE -> Compare.CompareOp.GT;
		case GE -> Compare.CompareOp.LT;
		case GT -> Compare.CompareOp.LE;
		};
	}

	public static void replaceIfRequired(Compare cmp, RawDictionary rd) {
		Compare.CompareOp op = cmp.getOperator();
		switch (op) {
		case EQ, NE -> {
			return;
		}
		}
		ValueExpr left = cmp.getLeftArg();
		ValueExpr right = cmp.getRightArg();

		boolean vcl = left instanceof ValueConstant;
		boolean vcr = right instanceof ValueConstant;

		if (vcl && vcr) {
			return; // waste of time
		}

		if (vcr) {
			// cst OP ?r
			ValueExpr left2 = RawNumberIfRequired((ValueConstant) left);
			if (left2 != null) {
				cmp.replaceWith(new HDTCompareOp(right, left2, inverse(op)));
			}
		} else {
			ValueExpr right2 = RawNumberIfRequired((ValueConstant) right);
			if (right2 != null) {
				cmp.replaceWith(new HDTCompareOp(left, right2, op));
			}
		}
	}

	private static ValueExpr RawNumberIfRequired(ValueConstant vc) {
		Value val = vc.getValue();
		if (!val.isLiteral())
			return null;

		Literal lit = (Literal) val;
		CoreDatatype cdt = lit.getCoreDatatype();

		if (!cdt.isXSDDatatype() || !cdt.asXSDDatatype().orElseThrow().isNumericDatatype()) {
			return null;
		}

		throw new NotImplementedException(); // TODO: do
	}

	private final Compare.CompareOp op;

	public HDTCompareOp(ValueExpr base, ValueExpr cst, Compare.CompareOp op) {
		super(base.clone(), cst.clone());
		this.op = op;
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
		visitor.meetOther(this);
	}

	public Compare.CompareOp getOp() {
		return op;
	}
}

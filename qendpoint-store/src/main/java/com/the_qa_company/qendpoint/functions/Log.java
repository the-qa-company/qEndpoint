package com.the_qa_company.qendpoint.functions;

import com.the_qa_company.qendpoint.store.EndpointStore;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

public class Log implements Function {
	@Override
	public String getURI() {
		return EndpointStore.BASE_URI + "log";
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length < 1 || args.length > 2) {
			throw new ValueExprEvaluationException(getURI() + "(value, base = 10)");
		}

		Value valueVal = args[0];

		if (!valueVal.isLiteral() || !(valueVal instanceof Literal lit)) {
			throw new ValueExprEvaluationException(getURI() + " : value should be a literal");
		}
		CoreDatatype cdt = lit.getCoreDatatype();

		if (!cdt.isXSDDatatype() || !cdt.asXSDDatatype().orElseThrow().isNumericDatatype()) {
			throw new ValueExprEvaluationException(getURI() + " : value should be a numeric literal");
		}

		double val = lit.doubleValue();

		if (val <= 0) {
			throw new ValueExprEvaluationException(getURI() + " : value should be a positive number");
		}

		int base;

		if (args.length >= 2) {
			Value baseVal = args[1];

			if (!baseVal.isLiteral() || !(baseVal instanceof Literal litb)) {
				throw new ValueExprEvaluationException(getURI() + " : base should be a literal");
			}
			CoreDatatype bcdt = litb.getCoreDatatype();

			if (!bcdt.isXSDDatatype() || !bcdt.asXSDDatatype().orElseThrow().isIntegerDatatype()) {
				throw new ValueExprEvaluationException(getURI() + " : base should be an int literal");
			}

			base = litb.intValue();
		} else {
			base = 10;
		}
		if (base == 10) {
			return valueFactory.createLiteral(Math.log10(val));
		}
		return valueFactory.createLiteral(Math.log(val) / Math.log(base));
	}
}

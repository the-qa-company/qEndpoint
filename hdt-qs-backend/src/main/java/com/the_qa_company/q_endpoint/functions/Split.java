package com.the_qa_company.q_endpoint.functions;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.spin.function.InverseMagicProperty;

import java.util.Iterator;
import java.util.List;

public class Split implements TupleFunction {

    @Override
    public String getURI() {
        return "http://qanswer.eu/function/split";
    }

    @Override
    public CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> evaluate(
            final ValueFactory valueFactory, Value... args) throws QueryEvaluationException {
        if (args.length != 2) {
            throw new ValueExprEvaluationException(
                    String.format("%s requires 2 arguments, got %d", getURI(), args.length));
        }
        if (!(args[0] instanceof Literal)) {
            throw new ValueExprEvaluationException("First list element must be a literal");
        }
        if (!(args[1] instanceof Literal)) {
            throw new ValueExprEvaluationException("Second list element must be a literal");
        }
        final String s = ((Literal) args[0]).stringValue();
        final String regex = ((Literal) args[1]).stringValue();
        final String[] parts = s.split(regex);
        return new CloseableIteratorIteration(
                new Iterator<Value>() {

                    int pos = 0;

                    @Override
                    public boolean hasNext() {
                        return (pos < parts.length);
                    }

                    @Override
                    public Value next() {
                        return valueFactory.createLiteral(parts[pos++]);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                });
    }
}

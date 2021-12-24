package com.the_qa_company.q_endpoint.utils;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.TripleID;

public class HDTEvaluationStatistics extends EvaluationStatistics {
    private final HDT hdt;

    public HDTEvaluationStatistics(HDT hdt) {
        this.hdt = hdt;
    }

    @Override
    protected CardinalityCalculator createCardinalityCalculator() {
        return new HDTCardinalityCalculator();
    }

    protected class HDTCardinalityCalculator extends CardinalityCalculator {

        @Override
        public double getCardinality(StatementPattern sp) {
            Value subject = getConstantValue(sp.getSubjectVar());
            Value predicate = getConstantValue(sp.getPredicateVar());
            Value object = getConstantValue(sp.getObjectVar());

            long subId = 0;
            long predId = 0;
            long objId = 0;

            IRIConverter iriConverter = new IRIConverter(hdt);
            if (subject != null) {
                subId = iriConverter.convertSubj((Resource) subject);
            }
            if (predicate != null) {
                predId = iriConverter.convertPred((IRI) predicate);
            }
            if (object != null) {
                objId = iriConverter.convertObj(object);
            }
            double cardinality;

            if (subId == 0 && predId == 0 && objId == 0) {
        /*
        apparently we got all variables in the triple so we'll not search the whole knowledge base
        to get the cardinality so put we put a high card to put this triple on last in the ordering scenario
        */
                cardinality = Double.MAX_VALUE;
            } else {

                cardinality =
                        hdt.getTriples().search(new TripleID(subId, predId, objId)).estimatedNumResults();
            }

            return cardinality;
        }

        protected Value getConstantValue(Var var) {
            if (var != null) {
                return var.getValue();
            }

            return null;
        }
    }
}

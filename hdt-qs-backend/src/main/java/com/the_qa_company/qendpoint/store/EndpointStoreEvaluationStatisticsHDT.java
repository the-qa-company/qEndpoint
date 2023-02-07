package com.the_qa_company.qendpoint.store;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import com.the_qa_company.qendpoint.core.triples.TripleID;

public class EndpointStoreEvaluationStatisticsHDT extends EvaluationStatistics {
	private final EndpointStore endpoint;

	public EndpointStoreEvaluationStatisticsHDT(EndpointStore endpoint) {
		this.endpoint = endpoint;
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

			HDTConverter hdtConverter = new HDTConverter(endpoint);
			long subId = hdtConverter.subjectToID((Resource) subject);
			long predId = hdtConverter.predicateToID((IRI) predicate);
			long objId = hdtConverter.objectToID(object);

			double cardinality;

			if (subId == 0 && predId == 0 && objId == 0) {
				/*
				 * apparently we got all variables in the triple so we'll not
				 * search the whole knowledge base to get the cardinality so put
				 * we put a high card to put this triple on last in the ordering
				 * scenario
				 */
				cardinality = Double.MAX_VALUE;
			} else {
				cardinality = endpoint.getHdt().getTriples().search(new TripleID(subId, predId, objId))
						.estimatedNumResults();
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

package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

public class EndpointStoreEvaluationStatisticsHDT extends EvaluationStatistics {
	private final EndpointStore endpoint;
	private final HDTConverter hdtConverter;
	private final HDTCardinalityCalculator calculator = new HDTCardinalityCalculator();

	public EndpointStoreEvaluationStatisticsHDT(EndpointStore endpoint) {
		this.endpoint = endpoint;
		this.hdtConverter = endpoint.getHdtConverter();
	}

	@Override
	protected CardinalityCalculator createCardinalityCalculator() {
		return calculator;
	}

	protected class HDTCardinalityCalculator extends CardinalityCalculator {

		@Override
		public double getCardinality(StatementPattern sp) {
			Value subject = getConstantValue(sp.getSubjectVar());
			Value predicate = getConstantValue(sp.getPredicateVar());
			Value object = getConstantValue(sp.getObjectVar());

			long subId = hdtConverter.subjectToID((Resource) subject);
			long predId = hdtConverter.predicateToID((IRI) predicate);
			long objId = hdtConverter.objectToID(object);

			if (subId == 0 && predId == 0 && objId == 0) {
				/*
				 * apparently we got all variables in the triple so we'll not
				 * search the whole knowledge base to get the cardinality so put
				 * we put a high card to put this triple on last in the ordering
				 * scenario
				 */
				return Double.MAX_VALUE;
			} else {
				TripleID tid = new TripleID(subId, predId, objId);

				IteratorTripleID it = endpoint.getHdt().getTriples().search(tid);
				switch (it.numResultEstimation()) {
				case APPROXIMATE, EXACT, UP_TO -> {
					double cardinality = it.estimatedNumResults();

					double multiplier = switch (tid.getPatternString()) {
					case "S??", "SP?", "SPO" -> 1; // SPO INDEX
					case "??O", "?PO" -> 2; // OPS INDEX
					case "?P?" -> 4; // P INDEX
					default -> 8; // no INDEX
					};

					return cardinality * multiplier;
				}
				case UNKNOWN, MORE_THAN -> {
					// We don't know
					return Double.MAX_VALUE;
				}
				default -> throw new AssertionError("Unknown estimation: " + it.numResultEstimation());
				}
			}
		}

		protected Value getConstantValue(Var var) {
			if (var.hasValue()) {
				return var.getValue();
			}

			return null;
		}
	}
}

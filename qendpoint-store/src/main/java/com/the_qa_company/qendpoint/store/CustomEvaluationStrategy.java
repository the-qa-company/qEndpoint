package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.model.HDTValue;
import com.the_qa_company.qendpoint.model.SimpleIRIHDT;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;

import java.util.Set;

public class CustomEvaluationStrategy extends DefaultEvaluationStrategy {

	private static final Log log = LogFactory.getLog(CustomEvaluationStrategy.class);

	public CustomEvaluationStrategy(TripleSource tripleSource, FederatedServiceResolver serviceResolver) {
		super(tripleSource, serviceResolver);
	}

	public CustomEvaluationStrategy(TripleSource tripleSource, Dataset dataset,
			FederatedServiceResolver serviceResolver) {
		super(tripleSource, dataset, serviceResolver);
	}

	public CustomEvaluationStrategy(TripleSource tripleSource, Dataset dataset,
			FederatedServiceResolver serviceResolver, long iterationCacheSyncTreshold,
			EvaluationStatistics evaluationStatistics) {
		super(tripleSource, dataset, serviceResolver, iterationCacheSyncTreshold, evaluationStatistics);
	}

	public CustomEvaluationStrategy(TripleSource tripleSource, Dataset dataset,
			FederatedServiceResolver serviceResolver, long iterationCacheSyncTreshold,
			EvaluationStatistics evaluationStatistics, boolean trackResultSize) {
		super(tripleSource, dataset, serviceResolver, iterationCacheSyncTreshold, evaluationStatistics,
				trackResultSize);
	}

	public CustomEvaluationStrategy(TripleSource tripleSource, Dataset dataset,
			FederatedServiceResolver serviceResolver, long iterationCacheSyncTreshold,
			EvaluationStatistics evaluationStatistics, boolean trackResultSize,
			TupleFunctionRegistry tupleFunctionRegistry) {
		super(tripleSource, dataset, serviceResolver, iterationCacheSyncTreshold, evaluationStatistics, trackResultSize,
				tupleFunctionRegistry);
	}

	@Override
	protected QueryEvaluationStep prepare(Join node, QueryEvaluationContext context) throws QueryEvaluationException {
		TupleExpr leftArg = node.getLeftArg();
		TupleExpr rightArg = node.getRightArg();

		String property = System.getProperty("prototypejoin");
		System.err.println("prototypejoin: " + property);
		if (property == null || !property.equals("true")) {
			System.out.println("prototypejoin is not enabled");
			return super.prepare(node, context);
		}

		try {
			if (leftArg instanceof StatementPattern left && rightArg instanceof StatementPattern right
					&& tripleSource instanceof EndpointTripleSource endpointTripleSource) {

				HDTValues leftValues = getHdtValues(left);
				HDTValues rightValues = getHdtValues(right);

				try {
					if (!left.getSubjectVar().getName().equals(right.getSubjectVar().getName())) {
						log.error("Subject names do not match");
						return super.prepare(node, context);
					}
				} catch (Exception e) {
					log.error("Subject names do not match");
					return super.prepare(node, context);
				}

				Set<String> bindingNames = left.getAssuredBindingNames();
				Set<String> bindingNames1 = right.getAssuredBindingNames();

				int common = 0;
				for (String name : bindingNames) {
					if (bindingNames1.contains(name)) {
						common++;
					}
				}

				if (common != 1) {
					log.error("Common bindings are not 1");
					return super.prepare(node, context);
				}

				if (!left.getPredicateVar().isConstant()) {
					log.error("Predicate is not constant");
					return super.prepare(node, context);
				}
				if (!right.getPredicateVar().isConstant()) {
					log.error("Predicate is not constant");
					return super.prepare(node, context);
				}

				if (left.getObjectVar().isConstant() && right.getObjectVar().isConstant()) {
					log.error("Both objects are constant");
					return super.prepare(node, context);
				}

				if (left.getContextVar() != null || right.getContextVar() != null) {
					log.error("we don't support contexts");
					return super.prepare(node, context);
				}

				if (!left.getObjectVar().isConstant() && !right.getObjectVar().isConstant()) {
					log.error("Both objects are variables");
					return super.prepare(node, context);
				}

				System.out.println("Left: " + left);
				System.out.println("Right: " + right);

				node.setAlgorithm("ProtoypeJoinIterator");

				// we will implement a very simple join on the subject
				return bindings -> {
					if (!bindings.isEmpty()) {
						throw new UnsupportedOperationException("Query bindings are not supported");
					}

					// we will implement a very simple join on the subject

					CloseableIteration<TripleID> statements = endpointTripleSource.prototypeGetStatements(null,
							leftValues.subject() != null ? leftValues.subject().getHDTId() : 0,
							leftValues.predicate() != null ? leftValues.predicate().getHDTId() : 0,
							leftValues.object() != null ? leftValues.object().getHDTId() : 0,
							leftValues.context() != null ? leftValues.context().getHDTId() : 0);

					return new ProtoypeJoinIterator(statements, rightValues.predicate(), rightValues.object(),
							rightValues.context(), endpointTripleSource, (StatementPattern) leftArg,
							(StatementPattern) rightArg);

				};

			}

			System.out.println();

		} catch (Exception e) {
			log.error("Error in prepare for prototype join", e);
		}

		return super.prepare(node, context);
	}

	private static HDTValues getHdtValues(StatementPattern left) {
		HDTValue leftSubject = left.getSubjectVar() != null ? (HDTValue) left.getSubjectVar().getValue() : null;
		SimpleIRIHDT leftPredicate = left.getPredicateVar() != null ? (SimpleIRIHDT) left.getPredicateVar().getValue()
				: null;
		HDTValue leftObject = left.getObjectVar() != null ? (HDTValue) left.getObjectVar().getValue() : null;
		HDTValue leftContext = left.getContextVar() != null ? (HDTValue) left.getContextVar().getValue() : null;
		return new HDTValues(leftSubject, leftPredicate, leftObject, leftContext);
	}

	private record HDTValues(HDTValue subject, SimpleIRIHDT predicate, HDTValue object, HDTValue context) {}
}

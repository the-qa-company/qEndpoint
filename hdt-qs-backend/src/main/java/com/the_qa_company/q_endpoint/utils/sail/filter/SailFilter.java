package com.the_qa_company.q_endpoint.utils.sail.filter;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.UpdateContext;

import java.util.function.BiFunction;

/**
 * A filter for a {@link com.the_qa_company.q_endpoint.utils.sail.FilteringSail}
 *
 * @author Antoine Willerval
 */
public interface SailFilter {
	/**
	 * test if an add operation should pass the filter
	 * @param op the context of the add (can be null)
	 * @param subj the subject of the add statement
	 * @param pred the predicate of the add statement
	 * @param obj the object of the add statement
	 * @param contexts the contexts of the add statement
	 * @return if the filter should send this request to the yes(true) or no(false) connection
	 */
	boolean shouldHandleAdd(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts);

	/**
	 * test if a remove operation should pass the filter
	 * @param op the context of the add (can be null)
	 * @param subj the subject of the add statement
	 * @param pred the predicate of the add statement
	 * @param obj the object of the add statement
	 * @param contexts the contexts of the add statement
	 * @return if the filter should send this request to the yes(true) or no(false) connection
	 */
	boolean shouldHandleRemove(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts);

	/**
	 * test if a get operation should pass the filter
	 * @param subj the subject of the add statement
	 * @param pred the predicate of the add statement
	 * @param obj the object of the add statement
	 * @param contexts the contexts of the add statement
	 * @return if the filter should send this request to the yes(true) or no(false) connection
	 */
	boolean shouldHandleGet(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts);

	/**
	 * test if an expression should pass the filter
	 * @param tupleExpr the expression
	 * @param dataset the dataset
	 * @param bindings the binding
	 * @param includeInferred if it should include the inferred
	 * @return if the filter should send this expression to the yes(true) or no(false) connection
	 */
	boolean shouldHandleExpression(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred);

	/**
	 * create operation of multiple filter value
	 * @param other the other filter to join
	 * @param operation the join operation
	 * @return the new join filter
	 */
	default SailFilter op(SailFilter other, BiFunction<Boolean, Boolean, Boolean> operation) {
		final SailFilter that = this;
		return new SailFilter() {
			@Override
			public boolean shouldHandleAdd(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
				return operation.apply(
						that.shouldHandleAdd(op, subj, pred, obj, contexts),
						other.shouldHandleAdd(op, subj, pred, obj, contexts)
				);
			}

			@Override
			public boolean shouldHandleRemove(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
				return operation.apply(
						that.shouldHandleRemove(op, subj, pred, obj, contexts),
						other.shouldHandleRemove(op, subj, pred, obj, contexts)
				);
			}

			@Override
			public boolean shouldHandleGet(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) {
				return operation.apply(
						that.shouldHandleGet(subj, pred, obj, includeInferred, contexts),
						other.shouldHandleGet(subj, pred, obj, includeInferred, contexts)
				);
			}

			@Override
			public boolean shouldHandleExpression(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) {
				return operation.apply(
						that.shouldHandleExpression(tupleExpr, dataset, bindings, includeInferred),
						other.shouldHandleExpression(tupleExpr, dataset, bindings, includeInferred)
				);
			}
		};
	}

	/**
	 * create the and operation of 2 filters
	 * @param other the other filter
	 * @return the new join filter
	 */
	default SailFilter and(SailFilter other) {
		return op(other, (b1, b2) -> b1 && b2);
	}

	/**
	 * create the or operation of 2 filters
	 * @param other the other filter
	 * @return the new join filter
	 */
	default SailFilter or(SailFilter other) {
		return op(other, (b1, b2) -> b1 || b2);
	}

}

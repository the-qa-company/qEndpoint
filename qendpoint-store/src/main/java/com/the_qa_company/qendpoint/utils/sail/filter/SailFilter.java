package com.the_qa_company.qendpoint.utils.sail.filter;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;

/**
 * A filter for a {@link com.the_qa_company.qendpoint.utils.sail.FilteringSail}
 *
 * @author Antoine Willerval
 */
public interface SailFilter extends AutoCloseable {
	/**
	 * test if an add operation should pass the filter
	 *
	 * @param op       the context of the add (can be null)
	 * @param subj     the subject of the add statement
	 * @param pred     the predicate of the add statement
	 * @param obj      the object of the add statement
	 * @param contexts the contexts of the add statement
	 * @return if the filter should send this request to the yes(true) or
	 *         no(false) connection
	 */
	boolean shouldHandleAdd(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts);

	/**
	 * test if a remove operation should pass the filter
	 *
	 * @param op       the context of the add (can be null)
	 * @param subj     the subject of the add statement
	 * @param pred     the predicate of the add statement
	 * @param obj      the object of the add statement
	 * @param contexts the contexts of the add statement
	 * @return if the filter should send this request to the yes(true) or
	 *         no(false) connection
	 */
	boolean shouldHandleRemove(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts);

	/**
	 * test if an add operation should pass the filter
	 *
	 * @param subj     the subject of the add statement
	 * @param pred     the predicate of the add statement
	 * @param obj      the object of the add statement
	 * @param contexts the contexts of the add statement
	 * @return if the filter should send this request to the yes(true) or
	 *         no(false) connection
	 */
	boolean shouldHandleNotifyAdd(Resource subj, IRI pred, Value obj, Resource... contexts);

	/**
	 * test if a remove operation should pass the filter
	 *
	 * @param subj     the subject of the add statement
	 * @param pred     the predicate of the add statement
	 * @param obj      the object of the add statement
	 * @param contexts the contexts of the add statement
	 * @return if the filter should send this request to the yes(true) or
	 *         no(false) connection
	 */
	boolean shouldHandleNotifyRemove(Resource subj, IRI pred, Value obj, Resource... contexts);

	/**
	 * test if a get operation should pass the filter
	 *
	 * @param subj     the subject of the add statement
	 * @param pred     the predicate of the add statement
	 * @param obj      the object of the add statement
	 * @param contexts the contexts of the add statement
	 * @return if the filter should send this request to the yes(true) or
	 *         no(false) connection
	 */
	boolean shouldHandleGet(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts);

	/**
	 * test if an expression should pass the filter
	 *
	 * @param tupleExpr       the expression
	 * @param dataset         the dataset
	 * @param bindings        the binding
	 * @param includeInferred if it should include the inferred
	 * @return if the filter should send this expression to the yes(true) or
	 *         no(false) connection
	 */
	boolean shouldHandleExpression(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred);

	/**
	 * create operation of multiple filter value
	 *
	 * @param other     the other filter to join
	 * @param operation the join operation
	 * @return the new join filter
	 */
	default SailFilter op(SailFilter other, OpSailFilter.BiBoolFunction operation) {
		return new OpSailFilter(this, other, operation);
	}

	/**
	 * create the and operation of 2 filters
	 *
	 * @param other the other filter
	 * @return the new join filter
	 */
	default SailFilter and(SailFilter other) {
		return op(other, OpSailFilter.AND);
	}

	/**
	 * create the or operation of 2 filters
	 *
	 * @param other the other filter
	 * @return the new join filter
	 */
	default SailFilter or(SailFilter other) {
		return op(other, OpSailFilter.OR);
	}

	@Override
	void close() throws SailException;
}

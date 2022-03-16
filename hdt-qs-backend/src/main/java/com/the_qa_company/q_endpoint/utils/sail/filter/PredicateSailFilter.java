package com.the_qa_company.q_endpoint.utils.sail.filter;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.UpdateContext;

/**
 * Implementation of {@link SailFilter} to filter statements by predicate
 *
 * @author Antoine Willerval
 */
public class PredicateSailFilter implements SailFilter {
	private IRI predicate;

	/**
	 * filter a sail with a predicate
	 * @param predicate the predicate to filter
	 */
	public PredicateSailFilter(IRI predicate) {
		this.predicate = predicate;
	}

	@Override
	public boolean shouldHandleAdd(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		return predicate.equals(pred);
	}

	@Override
	public boolean shouldHandleRemove(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		return predicate.equals(pred);
	}

	@Override
	public boolean shouldHandleNotifyAdd(Resource subj, IRI pred, Value obj, Resource... contexts) {
		return predicate.equals(pred);
	}

	@Override
	public boolean shouldHandleNotifyRemove(Resource subj, IRI pred, Value obj, Resource... contexts) {
		return predicate.equals(pred);
	}

	@Override
	public boolean shouldHandleGet(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) {
		return predicate.equals(pred);
	}

	@Override
	public boolean shouldHandleExpression(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) {
		return true;
	}

	public void setPredicate(IRI predicate) {
		this.predicate = predicate;
	}

	public IRI getPredicate() {
		return predicate;
	}
}

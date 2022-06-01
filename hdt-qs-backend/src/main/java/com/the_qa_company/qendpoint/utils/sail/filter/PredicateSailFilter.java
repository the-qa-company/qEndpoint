package com.the_qa_company.qendpoint.utils.sail.filter;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.UpdateContext;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link SailFilter} to filter statements by predicate
 *
 * @author Antoine Willerval
 */
public class PredicateSailFilter implements SailFilter {
	private IRI predicate;
	private Set<IRI> predicates;

	/**
	 * filter a sail with predicates
	 *
	 * @param predicates the predicates to filter
	 */
	public PredicateSailFilter(IRI... predicates) {
		this(List.of(predicates));
	}

	/**
	 * filter a sail with predicates
	 *
	 * @param predicates the predicates to filter
	 */
	public PredicateSailFilter(List<IRI> predicates) {
		setPredicates(predicates);
	}

	private boolean shouldHandle(IRI pred) {
		return predicates != null ? predicates.contains(pred) : predicate.equals(pred);
	}

	@Override
	public boolean shouldHandleAdd(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		return shouldHandle(pred);
	}

	@Override
	public boolean shouldHandleRemove(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		return shouldHandle(pred);
	}

	@Override
	public boolean shouldHandleNotifyAdd(Resource subj, IRI pred, Value obj, Resource... contexts) {
		return shouldHandle(pred);
	}

	@Override
	public boolean shouldHandleNotifyRemove(Resource subj, IRI pred, Value obj, Resource... contexts) {
		return shouldHandle(pred);
	}

	@Override
	public boolean shouldHandleGet(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) {
		return shouldHandle(pred);
	}

	@Override
	public boolean shouldHandleExpression(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
			boolean includeInferred) {
		return true;
	}

	public void setPredicate(IRI predicate) {
		this.predicate = predicate;
		this.predicates = null;
	}

	public void setPredicates(List<IRI> predicates) {
		if (predicates.size() == 0) {
			throw new IllegalArgumentException("empty predicate count for sail filter!");
		}
		if (predicates.size() == 1) {
			this.predicate = predicates.get(0);
			this.predicates = null;
		} else {
			this.predicate = null;
			this.predicates = new HashSet<>(predicates);
		}
	}

	public Set<IRI> getPredicate() {
		return predicates != null ? predicates : Set.of(predicate);
	}
}

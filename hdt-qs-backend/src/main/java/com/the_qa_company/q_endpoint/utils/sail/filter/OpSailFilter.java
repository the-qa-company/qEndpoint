package com.the_qa_company.q_endpoint.utils.sail.filter;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.UpdateContext;

public class OpSailFilter implements SailFilter {
	public static final BiBoolFunction OR = (b1,b2) -> b1 || b2;
	public static final BiBoolFunction AND = (b1,b2) -> b1 && b2;
	@FunctionalInterface
	public interface BiBoolFunction {
		boolean apply(boolean op1, boolean op2);
	}
	private final SailFilter filter1;
	private final SailFilter filter2;
	private final BiBoolFunction operation;

	public OpSailFilter(SailFilter filter1, SailFilter filter2, BiBoolFunction operation) {
		this.filter1 = filter1;
		this.filter2 = filter2;
		this.operation = operation;
	}

	public SailFilter getFilter1() {
		return filter1;
	}

	public SailFilter getFilter2() {
		return filter2;
	}

	public BiBoolFunction getOperation() {
		return operation;
	}

	@Override
	public boolean shouldHandleAdd(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		return operation.apply(
				filter1.shouldHandleAdd(op, subj, pred, obj, contexts),
				filter2.shouldHandleAdd(op, subj, pred, obj, contexts)
		);
	}

	@Override
	public boolean shouldHandleRemove(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		return operation.apply(
				filter1.shouldHandleRemove(op, subj, pred, obj, contexts),
				filter2.shouldHandleRemove(op, subj, pred, obj, contexts)
		);
	}

	@Override
	public boolean shouldHandleNotifyAdd(Resource subj, IRI pred, Value obj, Resource... contexts) {
		return operation.apply(
				filter1.shouldHandleNotifyAdd(subj, pred, obj, contexts),
				filter2.shouldHandleNotifyAdd(subj, pred, obj, contexts)
		);
	}

	@Override
	public boolean shouldHandleNotifyRemove(Resource subj, IRI pred, Value obj, Resource... contexts) {
		return operation.apply(
				filter1.shouldHandleNotifyRemove(subj, pred, obj, contexts),
				filter2.shouldHandleNotifyRemove(subj, pred, obj, contexts)
		);
	}

	@Override
	public boolean shouldHandleGet(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) {
		return operation.apply(
				filter1.shouldHandleGet(subj, pred, obj, includeInferred, contexts),
				filter2.shouldHandleGet(subj, pred, obj, includeInferred, contexts)
		);
	}

	@Override
	public boolean shouldHandleExpression(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) {
		return operation.apply(
				filter1.shouldHandleExpression(tupleExpr, dataset, bindings, includeInferred),
				filter2.shouldHandleExpression(tupleExpr, dataset, bindings, includeInferred)
		);
	}
}

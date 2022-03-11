package com.the_qa_company.q_endpoint.utils.sail.filter;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link SailFilter} implementation to filter subject by type
 */
public class TypeSailFilter implements SailFilter {
	private final Map<Resource, Value> typeBuffer;
	private final Map<Resource, Boolean> typeContainedBuffer = new HashMap<>();
	private final IRI predicate;
	private final Value object;
	private final SailConnection subConnection;

	public TypeSailFilter(SailConnection subConnection, IRI predicate, Value object) {
		this(null, subConnection, predicate, object);
	}
	
	public TypeSailFilter(Map<Resource, Value> typeBuffer, SailConnection subConnection, IRI predicate, Value object) {
		this.typeBuffer = typeBuffer;
		this.subConnection = subConnection;
		this.predicate = predicate;
		this.object = object;
	}

	private boolean isSubjectOfType(Resource subj) {
		Boolean typeContainer = typeContainedBuffer.get(subj);

		if (typeContainer == null) {
			Value type = typeBuffer == null ? null : typeBuffer.get(subj);
			if (type == null) {
				try (CloseableIteration<? extends Statement, SailException> it = subConnection.getStatements(subj, predicate, null, false)) {
					if (it.hasNext()) {
						type = it.next().getObject();
						if (type == null) {
							return false;
						}

						if (typeBuffer != null) {
							typeBuffer.put(subj, type);
						}
					} else {
						return false;
					}
				}
			}
			typeContainer = type.equals(object);
			typeContainedBuffer.put(subj, typeContainer);
		}

		return typeContainer;
	}

	@Override
	public boolean shouldHandleAdd(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		if (pred.equals(predicate)) {
			if (typeBuffer != null) {
				typeBuffer.put(subj, obj);
			}
			typeContainedBuffer.put(subj, obj.equals(object));
			return false;
		}
		return isSubjectOfType(subj);
	}

	@Override
	public boolean shouldHandleRemove(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		if (pred.equals(predicate)) {
			if (typeBuffer != null) {
				typeBuffer.remove(subj);
			}
			// the type is removed from the triples, so obviously the answer is no
			typeContainedBuffer.put(subj, false);
			return false;
		}
		return isSubjectOfType(subj);
	}

	@Override
	public boolean shouldHandleGet(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) {
		return true; // no type filtering on get
	}

	@Override
	public boolean shouldHandleExpression(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) {
		return true; // no type filtering on evaluate
	}
}

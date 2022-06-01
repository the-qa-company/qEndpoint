package com.the_qa_company.qendpoint.utils.sail.filter;

import com.the_qa_company.qendpoint.utils.sail.FilteringSail;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link SailFilter} implementation to filter subject by type when adding by
 * notification
 *
 * @author Antoine Willerval
 */
public class TypeSailFilter implements SailFilter {
	private final Map<Resource, Value> typeBuffer;
	private final Map<Resource, Boolean> typeContainedBuffer = new HashMap<>();
	private final IRI predicate;
	private final Set<Value> objects;
	private final SailConnection subConnection;

	/**
	 * create a type sail filter
	 *
	 * @param filteringSail the connection to fetch data
	 * @param predicate     the predicate to define the type
	 * @param objects       the type object
	 */
	public TypeSailFilter(FilteringSail filteringSail, IRI predicate, Value... objects) {
		this(null, filteringSail, predicate, objects);
	}

	/**
	 * create a type sail filter
	 *
	 * @param filteringSail the connection to fetch data
	 * @param predicate     the predicate to define the type
	 * @param objects       the type object
	 */
	public TypeSailFilter(FilteringSail filteringSail, IRI predicate, List<Value> objects) {
		this(null, filteringSail, predicate, objects);
	}

	/**
	 * create a type sail filter with a buffer to store the type of the subjects
	 *
	 * @param typeBuffer    the buffer to store the subjects' type
	 * @param filteringSail the connection to fetch data
	 * @param predicate     the predicate to define the type
	 * @param objects       the type objects
	 */
	public TypeSailFilter(Map<Resource, Value> typeBuffer, FilteringSail filteringSail, IRI predicate,
			Value... objects) {
		this(typeBuffer, filteringSail, predicate, List.of(objects));
	}

	/**
	 * create a type sail filter with a buffer to store the type of the subjects
	 *
	 * @param typeBuffer    the buffer to store the subjects' type
	 * @param filteringSail the connection to fetch data
	 * @param predicate     the predicate to define the type
	 * @param objects       the type objects
	 */
	public TypeSailFilter(Map<Resource, Value> typeBuffer, FilteringSail filteringSail, IRI predicate,
			List<Value> objects) {
		this.typeBuffer = typeBuffer;
		this.subConnection = filteringSail.getOnNoSail().getConnectionInternal();
		this.predicate = predicate;
		if (objects.size() == 0) {
			throw new IllegalArgumentException("object type can't be empty!");
		}
		if (objects.size() == 1) {
			this.objects = Set.of(objects.get(0));
		} else {
			this.objects = new HashSet<>(objects);
		}
	}

	/**
	 * is the type of this subject filtered?
	 *
	 * @param subj the subject to try
	 * @return true if we have the triple (?subj predicate object) in the
	 *         subConnection, false otherwise
	 */
	private boolean isSubjectOfType(Resource subj) {
		Boolean typeContainer = typeContainedBuffer.get(subj);

		// did we already saw this subject in the connection?
		if (typeContainer == null) {
			// use the type buffer if given
			Value type = typeBuffer == null ? null : typeBuffer.get(subj);
			// did we already saw the type in the connection?
			if (type == null) {
				// query the type
				try (CloseableIteration<? extends Statement, SailException> it = subConnection.getStatements(subj,
						predicate, null, false)) {
					if (it.hasNext()) {
						type = it.next().getObject();
						if (type == null) {
							return false;
						}

						// if we can buffer it, we buffer it
						if (typeBuffer != null) {
							typeBuffer.put(subj, type);
						}
					} else {
						// no matching triple
						typeContainedBuffer.put(subj, false);
						return false;
					}
				}
			}
			// test the selected type
			typeContainer = objects.contains(type);
			typeContainedBuffer.put(subj, typeContainer);
		}

		return typeContainer;
	}

	@Override
	public boolean shouldHandleNotifyAdd(Resource subj, IRI pred, Value obj, Resource... contexts) {
		// ignore type triple and buffer the type
		if (pred.equals(predicate)) {
			if (typeBuffer != null) {
				typeBuffer.put(subj, obj);
			}
			typeContainedBuffer.put(subj, objects.contains(obj));
			return false;
		}
		return isSubjectOfType(subj);
	}

	@Override
	public boolean shouldHandleAdd(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		isSubjectOfType(subj); // prepare connection if required
		return false;
	}

	@Override
	public boolean shouldHandleNotifyRemove(Resource subj, IRI pred, Value obj, Resource... contexts) {
		// ignore type triple and buffer the type
		if (pred.equals(predicate)) {
			if (typeBuffer != null) {
				typeBuffer.remove(subj);
			}
			// the type is removed from the triples, so obviously the answer is
			// no
			typeContainedBuffer.put(subj, false);
			return false;
		}
		return true;
	}

	@Override
	public boolean shouldHandleRemove(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		return false;
	}

	@Override
	public boolean shouldHandleGet(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) {
		return true; // no type filtering on get
	}

	@Override
	public boolean shouldHandleExpression(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
			boolean includeInferred) {
		return true; // no type filtering on evaluate
	}
}

package com.the_qa_company.qendpoint.utils.sail;

import com.the_qa_company.qendpoint.utils.sail.filter.SailFilter;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.eclipse.rdf4j.sail.UpdateContext;

import java.util.Objects;

class FilteringSailConnection implements NotifyingSailConnection, SourceSailConnectionWrapper {
	private final NotifyingSailConnection connectionIfYes;
	private final MultiInputFilteringSailConnection connectionIfNo;
	private final SailFilter filter;

	public FilteringSailConnection(NotifyingSailConnection connectionIfYes,
			MultiInputFilteringSailConnection connectionIfNo, FilteringSail sail) {
		this.connectionIfYes = Objects.requireNonNull(connectionIfYes, "connectionIfYes can't be null!");
		this.connectionIfNo = Objects.requireNonNull(connectionIfNo, "connectionIfNo can't be null!");
		this.filter = Objects.requireNonNull(sail, "sail can't be null!").getFilter();
	}

	@Override
	public SailConnection getWrapped() {
		return connectionIfNo.getWrappedConnection();
	}

	@Override
	public boolean isOpen() throws SailException {
		return connectionIfYes.isOpen();
	}

	@Override
	public void close() throws SailException {
		try {
			connectionIfYes.close();
		} finally {
			filter.close();
		}
	}

	@Override
	public CloseableIteration<? extends BindingSet> evaluate(TupleExpr tupleExpr,
			Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
		if (filter.shouldHandleExpression(tupleExpr, dataset, bindings, includeInferred)) {
			return connectionIfYes.evaluate(tupleExpr, dataset, bindings, includeInferred);
		} else {
			return connectionIfNo.evaluate(tupleExpr, dataset, bindings, includeInferred);
		}
	}

	@Override
	public CloseableIteration<? extends Resource> getContextIDs() throws SailException {
		return connectionIfYes.getContextIDs();
	}

	@Override
	public CloseableIteration<? extends Statement> getStatements(Resource subj, IRI pred, Value obj,
			boolean includeInferred, Resource... contexts) throws SailException {
		if (filter.shouldHandleGet(subj, pred, obj, includeInferred, contexts)) {
			return connectionIfYes.getStatements(subj, pred, obj, includeInferred, contexts);
		} else {
			return connectionIfNo.getStatements(subj, pred, obj, includeInferred, contexts);
		}
	}

	@Override
	public long size(Resource... contexts) throws SailException {
		return connectionIfYes.size(contexts);
	}

	@Override
	public void begin() throws SailException {
		connectionIfYes.begin();
	}

	@Override
	public void begin(IsolationLevel level) throws SailException {
		connectionIfYes.begin(level);
	}

	@Override
	public void flush() throws SailException {
		connectionIfYes.flush();
	}

	@Override
	public void prepare() throws SailException {
		connectionIfYes.prepare();
	}

	@Override
	public void commit() throws SailException {
		connectionIfYes.commit();
	}

	@Override
	public void rollback() throws SailException {
		connectionIfYes.rollback();
	}

	@Override
	public boolean isActive() throws UnknownSailTransactionStateException {
		return connectionIfYes.isActive();
	}

	@Override
	public void addStatement(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		addStatement(null, subj, pred, obj, contexts);
	}

	@Override
	public void removeStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		removeStatement(null, subj, pred, obj, contexts);
	}

	@Override
	public void startUpdate(UpdateContext op) throws SailException {
		connectionIfYes.startUpdate(op);
	}

	@Override
	public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts)
			throws SailException {
		if (filter.shouldHandleAdd(op, subj, pred, obj, contexts)) {
			connectionIfYes.addStatement(op, subj, pred, obj, contexts);
		} else {
			connectionIfNo.addStatement(op, subj, pred, obj, contexts);
		}
	}

	@Override
	public void removeStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts)
			throws SailException {
		if (filter.shouldHandleRemove(op, subj, pred, obj, contexts)) {
			connectionIfYes.removeStatement(op, subj, pred, obj, contexts);
		} else {
			connectionIfNo.removeStatement(op, subj, pred, obj, contexts);
		}
	}

	@Override
	public void endUpdate(UpdateContext op) throws SailException {
		connectionIfYes.endUpdate(op);
	}

	@Override
	public void clear(Resource... contexts) throws SailException {
		connectionIfYes.clear(contexts);
	}

	@Override
	public CloseableIteration<? extends Namespace> getNamespaces() throws SailException {
		return connectionIfYes.getNamespaces();
	}

	@Override
	public String getNamespace(String prefix) throws SailException {
		return connectionIfYes.getNamespace(prefix);
	}

	@Override
	public void setNamespace(String prefix, String name) throws SailException {
		connectionIfYes.setNamespace(prefix, name);
	}

	@Override
	public void removeNamespace(String prefix) throws SailException {
		connectionIfYes.removeNamespace(prefix);
	}

	@Override
	public void clearNamespaces() throws SailException {
		connectionIfYes.clearNamespaces();
	}

	@Override
	public void addConnectionListener(SailConnectionListener listener) {
		connectionIfYes.addConnectionListener(listener);
		connectionIfNo.addBypassConnectionListener(listener);
	}

	@Override
	public void removeConnectionListener(SailConnectionListener listener) {
		connectionIfYes.removeConnectionListener(listener);
		connectionIfNo.removeBypassConnectionListener(listener);
	}

	/**
	 * @return the filter of the connection
	 */
	public SailFilter getFilter() {
		return filter;
	}
}

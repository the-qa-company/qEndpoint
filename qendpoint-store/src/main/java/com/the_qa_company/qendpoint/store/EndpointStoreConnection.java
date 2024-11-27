package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.compiler.ConfigSailConnection;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.store.exception.EndpointTimeoutException;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategyFactory;
import org.eclipse.rdf4j.query.explanation.Explanation;
import org.eclipse.rdf4j.rio.ntriples.NTriplesWriter;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.SailReadOnlyException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class EndpointStoreConnection extends SailSourceConnection implements ConfigSailConnection {
	private static final Timer TIMEOUT_TIMER = new Timer("EndpointStoreConnectionTimer", true);
	static long debugWaittime = 0L;
	private static final AtomicLong DEBUG_ID_STORE = new AtomicLong();
	private static final Logger logger = LoggerFactory.getLogger(EndpointStoreConnection.class);
	private final EndpointTripleSource tripleSource;
	private final EndpointStoreQueryPreparer queryPreparer;
	private boolean isWriteConnection = false;
	private final EndpointStore endpoint;
	NotifyingSailConnection connA_read;
	NotifyingSailConnection connB_read;
	NotifyingSailConnection connA_write;
	NotifyingSailConnection connB_write;
	private final long debugId;
	private final Lock connectionLock;
	private Lock updateLock;
	private CloseTask closeTask;
	private final AtomicBoolean timeout = new AtomicBoolean();
	private final Map<String, String> config = new HashMap<>();

	public EndpointStoreConnection(EndpointStore endpoint) throws InterruptedException {
		super(endpoint, endpoint.getCurrentSailStore(), new CustomEvaluationStrategyFactory());
		this.debugId = DEBUG_ID_STORE.getAndIncrement();
		this.endpoint = endpoint;
		EndpointStoreUtils.openConnection(this);
		// lock logic is here so that the connections is blocked
		this.endpoint.lockToPreventNewConnections.waitForActiveLocks();
		if (MergeRunnableStopPoint.disableRequest) {
			throw new MergeRunnableStopPoint.MergeRunnableException("connections request disabled");
		}
		this.connectionLock = this.endpoint.locksHoldByConnections.createLock("connection-lock");

		this.connA_read = endpoint.getNativeStoreA().getConnection();
		try {
			this.connB_read = endpoint.getNativeStoreB().getConnection();
			try {
				this.connA_write = endpoint.getNativeStoreA().getConnection();
				try {
					this.connB_write = endpoint.getNativeStoreB().getConnection();
				} catch (Throwable t) {
					try {
						connA_write.close();
					} catch (Throwable t2) {
						t.addSuppressed(t);
					}
					throw t;
				}
			} catch (Throwable t) {
				try {
					connB_read.close();
				} catch (Throwable t2) {
					t.addSuppressed(t);
				}
				throw t;
			}
		} catch (Throwable t) {
			try {
				connA_read.close();
			} catch (Throwable t2) {
				t.addSuppressed(t);
			}
			throw t;
		}

		// create the listener
		SailConnectionListener listener = new EndpointStoreConnectionListener();
		this.connA_read.addConnectionListener(listener);
		this.connA_write.addConnectionListener(listener);
		this.connB_read.addConnectionListener(listener);
		this.connB_write.addConnectionListener(listener);

		// each endpointStoreConnection has a triple source ( ideally it should
		// be in the query preparer as in rdf4j..)
		this.tripleSource = new EndpointTripleSource(this, endpoint);
		this.queryPreparer = new EndpointStoreQueryPreparer(endpoint, tripleSource, this);
	}

	@Override
	protected void notifyStatementAdded(Statement st) {
		try {
			endpoint.getLocksNotify().waitForActiveLocks();
		} catch (InterruptedException e) {
			throw new SailException(e);
		}
		HDTConverter converter = endpoint.getHdtConverter();
		super.notifyStatementAdded(converter.delegate(converter.rdf4ToHdt(st)));
	}

	@Override
	protected void notifyStatementRemoved(Statement st) {
		try {
			endpoint.getLocksNotify().waitForActiveLocks();
		} catch (InterruptedException e) {
			throw new SailException(e);
		}
		HDTConverter converter = endpoint.getHdtConverter();
		super.notifyStatementRemoved(converter.delegate(converter.rdf4ToHdt(st)));
	}

	@Override
	public void begin() throws SailException {
		logger.info("Begin connection transaction");

		super.begin();

		endpoint.mergeIfRequired();

		this.connA_write.begin();
		this.connB_write.begin();
	}

	// for SPARQL queries
	@Override
	protected CloseableIteration<? extends BindingSet> evaluateInternal(TupleExpr tupleExpr, Dataset dataset,
			BindingSet bindings, boolean includeInferred) throws SailException {
		return queryPreparer.evaluate(tupleExpr, dataset, bindings, includeInferred, 0);
	}

	@Override
	public Explanation explain(Explanation.Level level, TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
			boolean includeInferred, int timeoutSeconds) {
		try {
			queryPreparer.setExplanationLevel(level);
			return super.explain(level, tupleExpr, dataset, bindings, includeInferred, timeoutSeconds);
		} finally {
			queryPreparer.setExplanationLevel(null);
		}
	}

	// USED from connection get api not SPARQL
	@Override
	protected CloseableIteration<? extends Statement> getStatementsInternal(Resource subj, IRI pred, Value obj,
			boolean includeInferred, Resource... contexts) throws SailException {
		if (MergeRunnableStopPoint.disableRequest) {
			throw new MergeRunnableStopPoint.MergeRunnableException("connections request disabled");
		}

		if (debugWaittime != 0) {
			try {
				Thread.sleep(debugWaittime);
			} catch (InterruptedException e) {
				throw new AssertionError("no interruption during sleep", e);
			}
		}

		if (timeout.get()) {
			throw new EndpointTimeoutException();
		}
		CloseableIteration<? extends Statement> result = tripleSource.getStatements(subj, pred, obj, contexts);

		return new ExceptionConvertingIteration<Statement, SailException>(result) {
			@Override
			protected SailException convert(RuntimeException e) {
				return new SailException(e);
			}
		};
	}

	@Override
	public void setNamespaceInternal(String prefix, String name) throws SailException {
		// super.setNamespaceInternal(prefix,name);
		this.getCurrentConnectionWrite().setNamespace(prefix, name);
		// this.getCurrentConnectionRead().setNamespace(prefix, name);
	}

	@Override
	public void setConfig(String cfg) {
		config.put(cfg, "");
	}

	@Override
	public void setConfig(String cfg, String value) {
		config.put(cfg, value);
	}

	@Override
	public boolean hasConfig(String cfg) {
		return config.containsKey(cfg);
	}

	@Override
	public String getConfig(String cfg) {
		return config.get(cfg);
	}

	@Override
	public boolean allowUpdate() {
		return true;
	}

	@Override
	public boolean isActive() throws UnknownSailTransactionStateException {
		return this.connA_write.isActive() || this.connB_write.isActive();
	}

	@Override
	public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts)
			throws SailException {
		if (MergeRunnableStopPoint.disableRequest)
			throw new MergeRunnableStopPoint.MergeRunnableException("connections request disabled");

		isWriteConnection = true;

		// System.out.println(subj.stringValue()+" - "+ pred.stringValue() + " -
		// "+ obj.stringValue());
		Resource newSubj;
		IRI newPred;
		Value newObj;
		long subjectID = this.endpoint.getHdtConverter().subjectToID(subj);
		long predicateID = this.endpoint.getHdtConverter().predicateToID(pred);
		long objectID = this.endpoint.getHdtConverter().objectToID(obj);

		if (subjectID == -1) {
			newSubj = subj;
		} else {
			newSubj = this.endpoint.getHdtConverter().subjectIdToIRI(subjectID);
		}
		if (predicateID == -1) {
			newPred = pred;
		} else {
			newPred = this.endpoint.getHdtConverter().predicateIdToIRI(predicateID);
		}
		if (objectID == -1) {
			newObj = obj;
		} else {
			newObj = this.endpoint.getHdtConverter().objectIdToIRI(objectID);
		}

		if (!this.endpoint.getHdt().getDictionary().supportGraphs()) {
			// note that in the native store we insert a mix of native IRIs and
			// HDT
			// IRIs, depending if the resource is in
			// HDT or not
			TripleID tripleID = new TripleID(subjectID, predicateID, objectID);
			if (tripleDoesntExistInHDT(tripleID)) {
				// check if we need to search over the other native connection
				if (endpoint.isMerging()) {
					if (endpoint.shouldSearchOverRDF4J(subjectID, predicateID, objectID)) {
						try (CloseableIteration<? extends Statement> other = getOtherConnectionRead()
								.getStatements(newSubj, newPred, newObj, false)) {
							if (other.hasNext()) {
								return;
							}
						}
					}
				}
				// here we need uris using the internal IDs
				getCurrentConnectionWrite().addStatement(newSubj, newPred, newObj);

				// // modify the bitmaps if the IRIs used are in HDT
				this.endpoint.modifyBitmaps(subjectID, predicateID, objectID);
				// increase the number of statements
				this.endpoint.triplesCount++;
			}
		} else if (contexts.length <= 1) {
			long graphID;

			if (contexts.length != 0) {
				graphID = this.endpoint.getHdtConverter().subjectToID(contexts[0]);
			} else {
				graphID = this.endpoint.getHdtProps().getDefaultGraph();
			}

			Resource[] newGraph;
			if (graphID == -1) {
				newGraph = contexts;
			} else {
				newGraph = new Resource[] { this.endpoint.getHdtConverter().graphIdToIRI(graphID) };
			}
			TripleID tripleID = new TripleID(subjectID, predicateID, objectID, graphID);
			if (quadDoesntExistInHDT(tripleID)) {
				// check if we need to search over the other native connection
				if (endpoint.isMerging()) {
					if (endpoint.shouldSearchOverRDF4J(subjectID, predicateID, objectID)) {
						try (CloseableIteration<? extends Statement> other = getOtherConnectionRead()
								.getStatements(newSubj, newPred, newObj, false, newGraph)) {
							if (other.hasNext()) {
								return;
							}
						}
					}
				}
				// here we need uris using the internal IDs
				getCurrentConnectionWrite().addStatement(newSubj, newPred, newObj, newGraph);

				// modify the bitmaps if the IRIs used are in HDT
				this.endpoint.modifyBitmaps(subjectID, predicateID, objectID);
				// increase the number of statements
				this.endpoint.triplesCount++;
			}
		} else {
			Resource[] newGraph = new Resource[] { null };
			for (Resource context : contexts) {
				long graphID;
				if (context != null) {
					graphID = this.endpoint.getHdtConverter().subjectToID(context);
				} else {
					graphID = this.endpoint.getHdtProps().getDefaultGraph();
				}

				if (graphID == -1) {
					newGraph[0] = context;
				} else {
					newGraph[0] = this.endpoint.getHdtConverter().graphIdToIRI(graphID);
				}
				TripleID tripleID = new TripleID(subjectID, predicateID, objectID, graphID);
				if (quadDoesntExistInHDT(tripleID)) {
					// check if we need to search over the other native
					// connection
					if (endpoint.isMerging()) {
						if (endpoint.shouldSearchOverRDF4J(subjectID, predicateID, objectID)) {
							try (CloseableIteration<? extends Statement> other = getOtherConnectionRead()
									.getStatements(newSubj, newPred, newObj, false, newGraph)) {
								if (other.hasNext()) {
									continue;
								}
							}
						}
					}
					// here we need uris using the internal IDs
					getCurrentConnectionWrite().addStatement(newSubj, newPred, newObj, newGraph);

					// modify the bitmaps if the IRIs used are in HDT
					this.endpoint.modifyBitmaps(subjectID, predicateID, objectID);
					// increase the number of statements
					this.endpoint.triplesCount++;
				}
			}
		}
	}

	// @TODO: I think this is also not used because addStatement is used
	@Override
	public void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		this.getCurrentConnectionWrite().addStatement(subj, pred, obj, contexts);
	}

	@Override
	public void clearNamespacesInternal() throws SailException {
		// super.clearNamespacesInternal();
		getCurrentConnectionWrite().clearNamespaces();
	}

	@Override
	public void removeNamespaceInternal(String prefix) throws SailException {
		// super.removeNamespaceInternal(prefix);
		getCurrentConnectionWrite().removeNamespace(prefix);
	}

	@Override
	protected void clearInternal(Resource... contexts) throws SailException {
		getCurrentConnectionWrite().clear(contexts);
	}

	@Override
	protected CloseableIteration<? extends Namespace> getNamespacesInternal() throws SailException {
		return getCurrentConnectionRead().getNamespaces();
	}

	@Override
	protected String getNamespaceInternal(String prefix) throws SailException {
		return getCurrentConnectionRead().getNamespace(prefix);
	}

	@Override
	protected void commitInternal() throws SailException {
		super.commitInternal();
		this.connA_write.commit();
		this.connB_write.commit();
	}

	@Override
	public void flush() throws SailException {
		super.flush();
		if (isWriteConnection) {
			try {
				endpoint.flushWrites();
			} catch (IOException e) {
				throw new SailException("Can't flush endpoint store writes", e);
			}
		}
		this.connA_write.flush();
		this.connB_write.flush();
	}

	@Override
	public void flushUpdates() throws SailException {
		super.flushUpdates();
		this.connA_write.flush();
		this.connB_write.flush();
	}

	@Override
	public void startUpdate(UpdateContext op) throws SailException {
		// @todo: is this not strange that both are prepared?
		this.connA_write.startUpdate(op);
		this.connB_write.startUpdate(op);
		this.connA_read.close();
		this.connB_read.close();
		this.connA_read = endpoint.getNativeStoreA().getConnection();
		this.connB_read = endpoint.getNativeStoreB().getConnection();

		logger.debug("Update started");
		try {
			endpoint.lockToPreventNewUpdate.waitForActiveLocks();
		} catch (InterruptedException e) {
			throw new SailException(e);
		}
		if (op != null) {
			updateLock = endpoint.locksHoldByUpdates.createLock("update #" + debugId);
		}
	}

	@Override
	protected void endUpdateInternal(UpdateContext op) throws SailException {
		// @todo: is this not strange that both are prepared?
		this.connA_write.endUpdate(op);
		this.connB_write.endUpdate(op);
		logger.debug("Update ended");
		if (op != null) {
			updateLock.release();
		}
	}

	@Override
	protected void rollbackInternal() throws SailException {
		getCurrentConnectionWrite().rollback();
	}

	@Override
	protected void closeInternal() throws SailException {
		logger.debug("Number of times native store was called:" + this.tripleSource.getCount());
		if (isWriteConnection) {
			try {
				endpoint.flushWrites();
			} catch (IOException e) {
				throw new SailException("Can't flush endpoint store writes", e);
			}
		}
		super.closeInternal();
		// this.nativeStoreConnection.close();
		this.connA_read.close();
		this.connB_read.close();
		this.connA_write.close();
		this.connB_write.close();
		if (closeTask != null) {
			closeTask.cancel();
		}
		this.connectionLock.release();
		EndpointStoreUtils.closeConnection(this);
	}

	@Override
	protected CloseableIteration<? extends Resource> getContextIDsInternal() throws SailException {
		return getCurrentConnectionRead().getContextIDs();
	}

	@Override
	protected long sizeInternal(Resource... contexts) throws SailException {
		// return endpoint.getNativeStoreConnection().size(contexts);
		long sizeNativeA = connA_read.size(contexts);
		long sizeNativeB = connB_read.size(contexts);
		long sizeHdt = this.endpoint.getHdt().getTriples().getNumberOfElements();

		long sizeDeleted = endpoint.isDeleteDisabled() ? 0
				: this.endpoint.getDeleteBitMap(TripleComponentOrder.SPO).getHandle().countOnes();
		logger.info("---------------------------");
		logger.info("Size native A:" + sizeNativeA);
		logger.info("Size native B:" + sizeNativeB);
		logger.info("Size deleted:" + sizeDeleted);
		logger.info("Size size HDT:" + sizeHdt);
		logger.info("---------------------------");
		return sizeHdt + sizeNativeA + sizeNativeB - sizeDeleted;
	}

	@Override
	public void removeStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts)
			throws SailException {
		if (MergeRunnableStopPoint.disableRequest)
			throw new MergeRunnableStopPoint.MergeRunnableException("connections request disabled");

		if (endpoint.isDeleteDisabled()) {
			throw new SailException("This sail doesn't support deletion");
		}

		isWriteConnection = true;

		Resource newSubj;
		IRI newPred;
		Value newObj;
		long subjectID = this.endpoint.getHdtConverter().subjectToID(subj);
		long predicateID = this.endpoint.getHdtConverter().predicateToID(pred);
		long objectID = this.endpoint.getHdtConverter().objectToID(obj);

		if (subjectID == -1) {
			newSubj = subj;
		} else {
			newSubj = this.endpoint.getHdtConverter().subjectIdToIRI(subjectID);
		}
		if (predicateID == -1) {
			newPred = pred;
		} else {
			newPred = this.endpoint.getHdtConverter().predicateIdToIRI(predicateID);
		}
		if (objectID == -1) {
			newObj = obj;
		} else {
			newObj = this.endpoint.getHdtConverter().objectIdToIRI(objectID);
		}

		TripleID tid = new TripleID(subjectID, predicateID, objectID);

		if (!this.endpoint.getHdt().getDictionary().supportGraphs()) {
			// remove statement from both stores... A and B
			if (endpoint.isMergeTriggered) {
				this.connA_write.removeStatement(op, newSubj, newPred, newObj, contexts);
				this.connB_write.removeStatement(op, newSubj, newPred, newObj, contexts);
			} else {
				this.getCurrentConnectionWrite().removeStatement(op, newSubj, newPred, newObj, contexts);
			}

			assignBitMapDeletes(tid, subj, pred, obj, contexts, null);
		} else {
			long[] contextIds = new long[contexts.length];
			Resource[] newcontexts = this.endpoint.getHdtConverter().graphIdToIRI(contexts, contextIds);

			// remove statement from both stores... A and B
			if (endpoint.isMergeTriggered) {
				this.connA_write.removeStatement(op, newSubj, newPred, newObj, newcontexts);
				this.connB_write.removeStatement(op, newSubj, newPred, newObj, newcontexts);
			} else {
				this.getCurrentConnectionWrite().removeStatement(op, newSubj, newPred, newObj, newcontexts);
			}

			assignBitMapDeletes(tid, subj, pred, obj, contexts, contextIds);
		}
	}

	// @todo: I think this is never used since it is not called in
	// removeStatement, not sure if this is good, since
	// there is some logic that we might miss
	@Override
	public void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... context) throws SailException {
		throw new SailReadOnlyException("");
	}

	private boolean tripleDoesntExistInHDT(TripleID tripleID) {
		IteratorTripleID iter = endpoint.getHdt().getTriples().search(tripleID);
		// if iterator is empty then the given triple doesn't exist in HDT
		if (iter.hasNext()) {
			TripleID tid = iter.next();
			if (endpoint.isDeleteDisabled()) {
				return false;
			}
			long index = iter.getLastTriplePosition();
			return this.endpoint
					.getDeleteBitMap(
							iter.isLastTriplePositionBoundToOrder() ? iter.getOrder() : TripleComponentOrder.SPO)
					.access(endpoint.getHdt().getDictionary().supportGraphs()
							? (tid.isQuad() ? tid.getGraph() : endpoint.getHdtProps().getDefaultGraph()) - 1
							: 0, index);
		}
		return true;
	}

	private boolean quadDoesntExistInHDT(TripleID tripleID) {
		IteratorTripleID iter = endpoint.getHdt().getTriples().search(tripleID);
		// if iterator is empty then the given triple 't' doesn't exist in HDT
		if (iter.hasNext()) {
			TripleID tid = iter.next();
			if (endpoint.isDeleteDisabled()) {
				return false;
			}
			long index = iter.getLastTriplePosition();
			return this.endpoint
					.getDeleteBitMap(
							iter.isLastTriplePositionBoundToOrder() ? iter.getOrder() : TripleComponentOrder.SPO)
					.access((tid.isQuad() ? tid.getGraph() : endpoint.getHdtProps().getDefaultGraph()) - 1, index);
		}
		return true;
	}

	private void assignBitMapDeletes(TripleID tid, Resource subj, IRI pred, Value obj, Resource[] contexts,
			long[] contextIds) throws SailException {
		if (endpoint.isDeleteDisabled()) {
			throw new SailException("This endpoint doesn't support deletion");
		}
		long s = tid.getSubject();
		long p = tid.getPredicate();
		long o = tid.getObject();

		TripleID tripleID = new TripleID(s, p, o);
		boolean supportGraphs = endpoint.getHdt().getDictionary().supportGraphs();
		if (s != -1 && p != -1 && o != -1) {
			if (contexts.length == 0 || !supportGraphs) {
				if (supportGraphs) {
					tripleID.setGraph(0); // all patterns
				}
				for (TripleComponentOrder order : endpoint.getValidOrders()) {
					IteratorTripleID iter = endpoint.getHdt().getTriples().search(tripleID, order.mask);

					while (iter.hasNext()) {
						TripleID removedId = iter.next();
						long index = iter.getLastTriplePosition();

						assert iter.isLastTriplePositionBoundToOrder();
						TripleComponentOrder sorder = iter.getOrder();

						assert sorder == order;

						long layer;
						if (supportGraphs) {
							layer = removedId.getGraph() - 1;
						} else {
							layer = 0;
						}

						if (!this.endpoint.getDeleteBitMap(sorder).access(layer, index)) {
							this.endpoint.getDeleteBitMap(sorder).set(layer, index, true);
							if (this.endpoint.isMerging()) {
								this.endpoint.getTempDeleteBitMap(sorder).set(layer, index, true);
							}
							if (order == TripleComponentOrder.SPO) {
								notifyStatementRemoved(
										this.endpoint.getValueFactory().createStatement(subj, pred, obj));
							}
						}
					}
				}
			} else {
				for (int i = 0; i < contexts.length; i++) {
					Resource context = contexts[i];
					tripleID.setGraph(contextIds[i]);
					if (tripleID.getGraph() == -1) {
						continue; // bad context
					}
					for (TripleComponentOrder order : endpoint.getValidOrders()) {
						IteratorTripleID iter = endpoint.getHdt().getTriples().search(tripleID, order.mask);

						if (iter.hasNext()) {
							TripleID removedId = iter.next();
							long index = iter.getLastTriplePosition();

							assert iter.isLastTriplePositionBoundToOrder();
							TripleComponentOrder sorder = iter.getOrder();

							if (!this.endpoint.getDeleteBitMap(sorder).access(removedId.getGraph() - 1, index)) {
								this.endpoint.getDeleteBitMap(sorder).set(removedId.getGraph() - 1, index, true);
								if (this.endpoint.isMerging()) {
									this.endpoint.getTempDeleteBitMap(sorder).set(removedId.getGraph() - 1, index,
											true);
								}
								if (order == TripleComponentOrder.SPO) {
									notifyStatementRemoved(
											this.endpoint.getValueFactory().createStatement(subj, pred, obj, context));
								}
							}
						}
					}
				}
			}
		} else {
			// @todo: why is this important?
			// means that the triple doesn't exist in HDT - we have to dump it
			// while merging, this triple might be in
			// the newly generated HDT
			if (this.endpoint.isMerging()) {
				NTriplesWriter writer = this.endpoint.getRdfWriterTempTriples();
				if (writer != null) {
					synchronized (writer) {
						if (contexts.length == 0 || !supportGraphs) {
							writer.handleStatement(this.endpoint.getValueFactory().createStatement(subj, pred, obj));
						} else {
							for (Resource ctx : contexts) {
								writer.handleStatement(
										this.endpoint.getValueFactory().createStatement(subj, pred, obj, ctx));
							}
						}
					}
				} else {
					logger.error("Writer is null!!");
				}
			}
		}
	}

	public SailConnection getCurrentConnectionRead() {
		if (endpoint.switchStore) {
			// logger.debug("STORE B");
			return connB_read;
		} else {
			// logger.debug("STORE A");
			return connA_read;
		}
	}

	public SailConnection getCurrentConnectionWrite() {
		if (endpoint.switchStore) {
			// logger.debug("STORE B");
			return connB_write;
		} else {
			// logger.debug("STORE A");
			return connA_write;
		}
	}

	public SailConnection getOtherConnectionRead() {
		if (!endpoint.switchStore) {
			// logger.debug("STORE B");
			return connB_read;
		} else {
			// logger.debug("STORE A");
			return connA_read;
		}
	}

	public SailConnection getOtherConnectionWrite() {
		if (!endpoint.switchStore) {
			// logger.debug("STORE B");
			return connB_write;
		} else {
			// logger.debug("STORE A");
			return connA_write;
		}
	}

	public SailConnection getConnA_read() {
		return connA_read;
	}

	public SailConnection getConnB_read() {
		return connB_read;
	}

	// @todo: this logic is already present somewhere else should be moved to
	// the store
	private long convertToId(Value iri, TripleComponentRole position) {
		return endpoint.getHdt().getDictionary().stringToId(iri.toString(), position);
	}

	/**
	 * set a max timeout for this connection getStatement
	 *
	 * @param timeout timeout (in millis), 0 to unset a previous timeout
	 */
	public void setConnectionTimeout(long timeout) {
		if (closeTask != null) {
			closeTask.cancel();
			if (timeout > 0) {
				logger.warn("a timeout was already set for this connection");
			} else {
				closeTask = null;
				return; // no new timeout to set
			}
		}
		this.timeout.set(false);

		if (timeout <= 0) {
			return;
		}

		logger.debug("set timeout {}", timeout);
		closeTask = new CloseTask();
		TIMEOUT_TIMER.schedule(closeTask, timeout);

	}

	public boolean isTimeout() {
		return timeout.get();
	}

	public EndpointStore getEndpoint() {
		return endpoint;
	}

	long getDebugId() {
		return debugId;
	}

	private class CloseTask extends TimerTask {
		@Override
		public void run() {
			timeout.set(true);
		}
	}

	private class EndpointStoreConnectionListener implements SailConnectionListener {
		private boolean shouldHandle() {
			return !endpoint.isMerging() || !endpoint.isNotificationsFreeze();
		}

		@Override
		public void statementAdded(Statement st) {
			if (shouldHandle()) {
				EndpointStoreConnection.this.notifyStatementAdded(st);
			}
		}

		@Override
		public void statementRemoved(Statement st) {
			if (shouldHandle()) {
				EndpointStoreConnection.this.notifyStatementRemoved(st);
			}
		}
	}
}

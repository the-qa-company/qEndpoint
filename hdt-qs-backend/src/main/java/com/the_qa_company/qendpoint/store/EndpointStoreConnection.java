package com.the_qa_company.qendpoint.store;

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
import org.eclipse.rdf4j.query.QueryEvaluationException;
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
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class EndpointStoreConnection extends SailSourceConnection {
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

	public EndpointStoreConnection(EndpointStore endpoint) throws InterruptedException {
		super(endpoint, endpoint.getCurrentSaliStore(), new StrictEvaluationStrategyFactory());
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
		this.queryPreparer = new EndpointStoreQueryPreparer(endpoint, tripleSource);
	}

	@Override
	protected void notifyStatementAdded(Statement st) {
		HDTConverter converter = endpoint.getHdtConverter();
		super.notifyStatementAdded(converter.delegate(converter.rdf4ToHdt(st)));
	}

	@Override
	protected void notifyStatementRemoved(Statement st) {
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
	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr,
			Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
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
	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource subj, IRI pred,
			Value obj, boolean includeInferred, Resource... contexts) throws SailException {
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
		CloseableIteration<? extends Statement, QueryEvaluationException> result = tripleSource.getStatements(subj,
				pred, obj, contexts);

		return new ExceptionConvertingIteration<Statement, SailException>(result) {
			@Override
			protected SailException convert(Exception e) {
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

		logger.debug("Adding triple {} {} {}", newSubj.toString(), newPred.toString(), newObj.toString());

		// note that in the native store we insert a mix of native IRIs and HDT
		// IRIs, depending if the resource is in
		// HDT or not
		TripleID tripleID = getTripleID(subjectID, predicateID, objectID);
		if (!tripleExistInHDT(tripleID)) {
			// check if we need to search over the other native connection
			if (endpoint.isMerging()) {
				if (endpoint.shouldSearchOverRDF4J(subjectID, predicateID, objectID)) {
					try (CloseableIteration<? extends Statement, SailException> other = getOtherConnectionRead()
							.getStatements(newSubj, newPred, newObj, false, contexts)) {
						if (other.hasNext()) {
							return;
						}
					}
				}
			}
			// here we need uris using the internal IDs
			getCurrentConnectionWrite().addStatement(newSubj, newPred, newObj, contexts);

			// // modify the bitmaps if the IRIs used are in HDT
			this.endpoint.modifyBitmaps(subjectID, predicateID, objectID);
			// increase the number of statements
			this.endpoint.triplesCount++;
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
	protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal() throws SailException {
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
				throw new SailException("Can't flush enpoint store writes", e);
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
	public boolean pendingRemovals() {
		return false;
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
	protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal() throws SailException {
		return getCurrentConnectionRead().getContextIDs();
	}

	@Override
	protected long sizeInternal(Resource... contexts) throws SailException {
		// return endpoint.getNativeStoreConnection().size(contexts);
		long sizeNativeA = connA_read.size(contexts);
		long sizeNativeB = connB_read.size(contexts);
		long sizeHdt = this.endpoint.getHdt().getTriples().getNumberOfElements();

		long sizeDeleted = this.endpoint.getDeleteBitMap().countOnes();
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

		// logger.debug("Removing triple {} {}
		// {}",newSubj.toString(),newPred.toString(),newObj.toString());

		// remove statement from both stores... A and B
		if (endpoint.isMergeTriggered) {
			this.connA_write.removeStatement(op, newSubj, newPred, newObj, contexts);
			this.connB_write.removeStatement(op, newSubj, newPred, newObj, contexts);
		} else {
			this.getCurrentConnectionWrite().removeStatement(op, newSubj, newPred, newObj, contexts);
		}
		// this.endpoint.triplesCount--;

		TripleID tripleID = getTripleID(subjectID, predicateID, objectID);
		assignBitMapDeletes(tripleID, subj, pred, obj);
	}

	// @todo: I think this is never used since it is not called in
	// removeStatement, not sure if this is good, since
	// there is some logic that we might miss
	@Override
	public void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... context) throws SailException {
		throw new SailReadOnlyException("");
	}

	private TripleID getTripleID(long subjId, long predId, long objId) {
		return new TripleID(subjId, predId, objId);

	}

	private boolean tripleExistInHDT(TripleID tripleID) {

		IteratorTripleID iter = endpoint.getHdt().getTriples().search(tripleID);
		// if iterator is empty then the given triple 't' doesn't exist in HDT
		if (iter.hasNext()) {
			iter.next();
			long index = iter.getLastTriplePosition();
			return !this.endpoint.getDeleteBitMap().access(index);
		}
		return false;
	}

	private void assignBitMapDeletes(TripleID tripleID, Resource subj, IRI pred, Value obj) throws SailException {

		if (tripleID.getSubject() != -1 && tripleID.getPredicate() != -1 && tripleID.getObject() != -1) {
			IteratorTripleID iter = endpoint.getHdt().getTriples().search(tripleID);

			if (iter.hasNext()) {
				iter.next();
				long index = iter.getLastTriplePosition();

				if (!this.endpoint.getDeleteBitMap().access(index)) {
					this.endpoint.getDeleteBitMap().set(index, true);
					if (this.endpoint.isMerging())
						this.endpoint.getTempDeleteBitMap().set(index, true);
					notifyStatementRemoved(this.endpoint.getValueFactory().createStatement(subj, pred, obj));
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
						Statement st = this.endpoint.getValueFactory().createStatement(subj, pred, obj);
						logger.debug("add to RDFWriter: {}", st);
						writer.handleStatement(st);
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

package com.the_qa_company.q_endpoint.hybridstore;

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
import org.eclipse.rdf4j.rio.ntriples.NTriplesWriter;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.SailReadOnlyException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;

public class HybridStoreConnection extends SailSourceConnection {

    private static final AtomicLong DEBUG_ID_STORE = new AtomicLong();
    private static final Logger logger = LoggerFactory.getLogger(HybridStoreConnection.class);
    private final HybridTripleSource tripleSource;
    private final HybridQueryPreparer queryPreparer;
    private boolean isWriteConnection = false;
    HybridStore hybridStore;
    SailConnection connA_read;
    SailConnection connB_read;
    SailConnection connA_write;
    SailConnection connB_write;
    private Lock connectionLock;
    private Lock updateLock;
    private long debugId;

    public HybridStoreConnection(HybridStore hybridStore) {
        super(hybridStore, hybridStore.getCurrentSaliStore(), new StrictEvaluationStrategyFactory());
        this.debugId = DEBUG_ID_STORE.getAndIncrement();
        this.hybridStore = hybridStore;
        // lock logic is here so that the connections is blocked
        try {
            this.hybridStore.lockToPreventNewConnections.waitForActiveLocks();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (MergeRunnableStopPoint.disableRequest)
            throw new MergeRunnableStopPoint.MergeRunnableException("connections request disabled");
        this.connectionLock = this.hybridStore.locksHoldByConnections.createLock("connection-lock");

        this.connA_read = hybridStore.getNativeStoreA().getConnection();
        this.connB_read = hybridStore.getNativeStoreB().getConnection();
        this.connA_write = hybridStore.getNativeStoreA().getConnection();
        this.connB_write = hybridStore.getNativeStoreB().getConnection();
        // each hybridStoreConnection has a triple source ( ideally it should be in the query preparer as in rdf4j..)
        this.tripleSource = new HybridTripleSource(this, hybridStore);
        this.queryPreparer = new HybridQueryPreparer(hybridStore, tripleSource);
    }

    @Override
    public void begin() throws SailException {
        logger.info("Begin connection transaction");

        super.begin();

        hybridStore.mergeIfRequired();

        this.connA_write.begin();
        this.connB_write.begin();
    }

    // for SPARQL queries
    @Override
    protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
        // TODO: check max execution time
        return queryPreparer.evaluate(tupleExpr, dataset, bindings, includeInferred, 0);
    }

    // USED from connection get api not SPARQL
    @Override
    protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
        if (MergeRunnableStopPoint.disableRequest)
            throw new MergeRunnableStopPoint.MergeRunnableException("connections request disabled");
        CloseableIteration<? extends Statement, QueryEvaluationException> result =
                tripleSource.getStatements(subj, pred, obj, contexts);
        return new ExceptionConvertingIteration<Statement, SailException>(result) {
            @Override
            protected SailException convert(Exception e) {
                return new SailException(e);
            }
        };
    }

    @Override
    public void setNamespaceInternal(String prefix, String name) throws SailException {
//    super.setNamespaceInternal(prefix,name);
        this.getCurrentConnectionWrite().setNamespace(prefix, name);
        //this.getCurrentConnectionRead().setNamespace(prefix, name);
    }

    @Override
    public boolean isActive() throws UnknownSailTransactionStateException {
        return this.connA_write.isActive() || this.connB_write.isActive();
    }

    @Override
    public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        if (MergeRunnableStopPoint.disableRequest)
            throw new MergeRunnableStopPoint.MergeRunnableException("connections request disabled");

        isWriteConnection = true;

//        System.out.println(subj.stringValue()+" - "+ pred.stringValue() + " - "+ obj.stringValue());
        Resource newSubj;
        IRI newPred;
        Value newObj;
        long subjectID = this.hybridStore.getHdtConverter().subjectToID(subj);
        long predicateID = this.hybridStore.getHdtConverter().predicateToID(pred);
        long objectID = this.hybridStore.getHdtConverter().objectToID(obj);

        if (subjectID == -1){
            newSubj = subj;
        } else {
            newSubj = this.hybridStore.getHdtConverter().subjectIdToIRI(subjectID);
        }
        if (predicateID == -1){
            newPred = pred;
        } else {
            newPred = this.hybridStore.getHdtConverter().predicateIdToIRI(predicateID);
        }
        if (objectID == -1){
            newObj = obj;
        } else {
            newObj = this.hybridStore.getHdtConverter().objectIdToIRI(objectID);
        }

        logger.debug("Adding triple {} {} {}",newSubj.toString(),newPred.toString(),newObj.toString());

        // note that in the native store we insert a mix of native IRIs and HDT IRIs, depending if the resource is in HDT or not
        TripleID tripleID = getTripleID(subjectID, predicateID, objectID);
        if (!tripleExistInHDT(tripleID)) {
            // check if we need to search over the other native connection
            if (hybridStore.isMerging()) {
                if (hybridStore.shouldSearchOverRDF4J(subjectID, predicateID, objectID)) {
                    CloseableIteration<? extends Statement, SailException> other = getOtherConnectionRead().getStatements(
                            newSubj,
                            newPred,
                            newObj,
                            false,
                            contexts
                    );
                    if (other.hasNext())
                        return;
                }
            }
            // here we need uris using the internal IDs
            getCurrentConnectionWrite().addStatement(
                    newSubj,
                    newPred,
                    newObj,
                    contexts
            );
            notifyStatementAdded(this.hybridStore.getValueFactory().createStatement(subj, pred, obj));

//            // modify the bitmaps if the IRIs used are in HDT
            this.hybridStore.modifyBitmaps(subjectID, predicateID, objectID);
            // increase the number of statements
            this.hybridStore.triplesCount++;
        }
    }

    // @TODO: I think this is also not used because addStatement is used
    @Override
    public void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
            throws SailException {
        this.getCurrentConnectionWrite().addStatement(subj, pred, obj, contexts);
    }

    @Override
    public void clearNamespacesInternal() throws SailException {
        //super.clearNamespacesInternal();
        getCurrentConnectionWrite().clearNamespaces();
    }

    @Override
    public void removeNamespaceInternal(String prefix) throws SailException {
        //super.removeNamespaceInternal(prefix);
        getCurrentConnectionWrite().removeNamespace(prefix);
    }

    @Override
    protected void clearInternal(Resource... contexts) throws SailException {
        getCurrentConnectionWrite().clear(contexts);
    }

    @Override
    protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
            throws SailException {
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
                hybridStore.flushWrites();
            } catch (IOException e) {
                throw new SailException("Can't flush hybrid store writes", e);
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
        this.connA_read = hybridStore.getNativeStoreA().getConnection();
        this.connB_read = hybridStore.getNativeStoreB().getConnection();

        logger.debug("Update started");
        try {
            hybridStore.lockToPreventNewUpdate.waitForActiveLocks();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (op != null) {
            updateLock = hybridStore.locksHoldByUpdates.createLock("update #" + debugId);
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
                hybridStore.flushWrites();
            } catch (IOException e) {
                throw new SailException("Can't flush hybrid store writes", e);
            }
        }
        super.closeInternal();
        //this.nativeStoreConnection.close();
        this.connA_read.close();
        this.connB_read.close();
        this.connA_write.close();
        this.connB_write.close();
        this.connectionLock.release();
    }

    @Override
    protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
            throws SailException {
        return getCurrentConnectionRead().getContextIDs();
    }

    @Override
    protected long sizeInternal(Resource... contexts) throws SailException {
        //return hybridStore.getNativeStoreConnection().size(contexts);
        long sizeNativeA = connA_read.size(contexts);
        long sizeNativeB = connB_read.size(contexts);
        long sizeHdt = this.hybridStore.getHdt().getTriples().getNumberOfElements();

        long sizeDeleted = this.hybridStore.getDeleteBitMap().countOnes();
        logger.info("---------------------------");
        logger.info("Size native A:" + sizeNativeA);
        logger.info("Size native B:" + sizeNativeB);
        logger.info("Size deleted:" + sizeDeleted);
        logger.info("Size size HDT:" + sizeHdt);
        logger.info("---------------------------");
        return sizeHdt + sizeNativeA + sizeNativeB - sizeDeleted;
    }

    @Override
    public void removeStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        if (MergeRunnableStopPoint.disableRequest)
            throw new MergeRunnableStopPoint.MergeRunnableException("connections request disabled");

        isWriteConnection = true;

        Resource newSubj;
        IRI newPred;
        Value newObj;
        long subjectID = this.hybridStore.getHdtConverter().subjectToID(subj);
        long predicateID = this.hybridStore.getHdtConverter().predicateToID(pred);
        long objectID = this.hybridStore.getHdtConverter().objectToID(obj);

        if (subjectID == -1){
            newSubj = subj;
        } else {
            newSubj = this.hybridStore.getHdtConverter().subjectIdToIRI(subjectID);
        }
        if (predicateID == -1){
            newPred = pred;
        } else {
            newPred = this.hybridStore.getHdtConverter().predicateIdToIRI(predicateID);
        }
        if (objectID == -1){
            newObj = obj;
        } else {
            newObj = this.hybridStore.getHdtConverter().objectIdToIRI(objectID);
        }

//        logger.debug("Removing triple {} {} {}",newSubj.toString(),newPred.toString(),newObj.toString());

        // remove statement from both stores... A and B
        if (hybridStore.isMergeTriggered) {
            this.connA_write.removeStatement(op, newSubj, newPred, newObj, contexts);
            this.connB_write.removeStatement(op, newSubj, newPred, newObj, contexts);
        } else {
            this.getCurrentConnectionWrite().removeStatement(op, newSubj, newPred, newObj, contexts);
        }
        notifyStatementRemoved(this.hybridStore.getValueFactory().createStatement(subj, pred, obj));
//        this.hybridStore.triplesCount--;

        TripleID tripleID = getTripleID(subjectID, predicateID, objectID);
        assignBitMapDeletes(tripleID, subj, pred, obj);
    }

    // @todo: I think this is never used since it is not called in removeStatement, not sure if this is good, since there is some logic that we might miss
    @Override
    public void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... context)
            throws SailException {
        throw new SailReadOnlyException("");
    }

    private TripleID getTripleID(long subjId, long predId, long objId) {
        return new TripleID(subjId, predId, objId);

    }

    private boolean tripleExistInHDT(TripleID tripleID) {

        Iterator<TripleID> iter = hybridStore.getHdt().getTriples().searchWithId(tripleID);
        // if iterator is empty then the given triple 't' doesn't exist in HDT
        if (iter.hasNext()) {
            TripleID next = iter.next();
            long index = next.getIndex();
            if (index != 0) {
                boolean res = !this.hybridStore.getDeleteBitMap().access(index - 1);
                return res;
            }
        }
        return false;
    }

    private void assignBitMapDeletes(TripleID tripleID, Resource subj, IRI pred, Value obj) throws SailException {

        if (tripleID.getSubject() != -1 && tripleID.getPredicate() != -1 && tripleID.getObject() != -1) {
            Iterator<TripleID> iter = hybridStore.getHdt().getTriples().searchWithId(tripleID);
            long index = -1;

            if (iter.hasNext())
                index = iter.next().getIndex();
            if (index != -1) {
                this.hybridStore.getDeleteBitMap().set(index - 1, true);
                if (this.hybridStore.isMerging())
                    this.hybridStore.getTempDeleteBitMap().set(index - 1, true);
            }
        } else {
            // @todo: why is this important?
            // means that the triple doesn't exist in HDT - we have to dump it while merging, this triple might be in the newly generated HDT
            if (this.hybridStore.isMerging()) {
                NTriplesWriter writer = this.hybridStore.getRdfWriterTempTriples();
                if (writer != null) {
                    Statement st = this.hybridStore.getValueFactory().createStatement(subj, pred, obj);
                    logger.debug("add to RDFWriter: {}", st);
                    writer.handleStatement(st);
                } else {
                    logger.error("Writer is null!!");
                }
            }
        }
    }

    public SailConnection getCurrentConnectionRead() {
        if (hybridStore.switchStore) {
//            logger.debug("STORE B");
            return connB_read;
        } else {
//            logger.debug("STORE A");
            return connA_read;
        }
    }

    public SailConnection getCurrentConnectionWrite() {
        if (hybridStore.switchStore) {
//            logger.debug("STORE B");
            return connB_write;
        } else {
//            logger.debug("STORE A");
            return connA_write;
        }
    }
    public SailConnection getOtherConnectionRead() {
        if (!hybridStore.switchStore) {
//            logger.debug("STORE B");
            return connB_read;
        } else {
//            logger.debug("STORE A");
            return connA_read;
        }
    }

    public SailConnection getOtherConnectionWrite() {
        if (!hybridStore.switchStore) {
//            logger.debug("STORE B");
            return connB_write;
        } else {
//            logger.debug("STORE A");
            return connA_write;
        }
    }

    public SailConnection getConnA_read() {
        return connA_read;
    }

    public SailConnection getConnB_read() {
        return connB_read;
    }

    // @todo: this logic is already present somewhere else should be moved to the store
    private long convertToId(Value iri, TripleComponentRole position) {
        return hybridStore.getHdt().getDictionary().stringToId(iri.toString(), position);
    }
}

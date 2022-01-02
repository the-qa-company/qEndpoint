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
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategyFactory;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.rio.RDFWriter;
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

import java.util.Iterator;

public class HybridStoreConnection extends SailSourceConnection {

    private static final Logger logger = LoggerFactory.getLogger(HybridStoreConnection.class);
    private final HybridTripleSource tripleSource;
    private final HybridQueryPreparer queryPreparer;
    HybridStore hybridStore;
    SailConnection connA;
    SailConnection connB;
    private Lock connectionLock;

    public HybridStoreConnection(HybridStore hybridStore) {
        super(hybridStore, hybridStore.getCurrentSaliStore(), new StrictEvaluationStrategyFactory());
        this.hybridStore = hybridStore;
        // lock logic is here so that the connections is blocked
        try {
            this.hybridStore.lockToPreventNewConnections.waitForActiveLocks();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.connectionLock = this.hybridStore.locksHoldByConnections.createLock("connection-lock");

        this.connA = hybridStore.getNativeStoreA().getConnection();
        this.connB = hybridStore.getNativeStoreB().getConnection();
        // each hybridStoreConnection has a triple source ( ideally it should be in the query preparer as in rdf4j..)
        this.tripleSource = new HybridTripleSource(this, hybridStore);
        this.queryPreparer = new HybridQueryPreparer(hybridStore, tripleSource);
    }

    @Override
    public void begin() throws SailException {
        logger.info("Begin connection transaction");


        super.begin();

        long count = this.hybridStore.triplesCount;

        logger.info("--------------: " + count);

        // Merge only if threshold in native store exceeded and not merging with hdt
        if (count >= hybridStore.getThreshold() && !hybridStore.isMerging()) {
            logger.info("Merging..." + count);
            hybridStore.makeMerge();
        }
        this.connA.begin();
        this.connB.begin();
    }

    private void getCardinality() {
        ParsedTupleQuery query = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL,
                "select * where {?a ?b ?c. }", null);

        TupleExpr expr = query.getTupleExpr();
        double cardinality = this.hybridStore.getCurrentSaliStore().getEvaluationStatistics().getCardinality(
                expr
        );
        logger.info("Cardinality = " + cardinality);
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
        this.getCurrentConnection().setNamespace(prefix, name);
    }

    @Override
    public boolean isActive() throws UnknownSailTransactionStateException {
        return this.getCurrentConnection().isActive();
    }

    @Override
    public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
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
            // here we need uris using the internal IDs
            getCurrentConnection().addStatement(
                    newSubj,
                    newPred,
                    newObj,
                    contexts
            );

            // modify the bitmaps if the IRIs used are in HDT
            this.hybridStore.modifyBitmaps(subjectID, predicateID, objectID);
            // increase the number of statements
            this.hybridStore.triplesCount++;
        }
    }

    // @TODO: I think this is also not used because addStatement is used
    @Override
    public void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
            throws SailException {

        this.getCurrentConnection().addStatement(subj, pred, obj, contexts);
    }

    @Override
    public void clearNamespacesInternal() throws SailException {
        //super.clearNamespacesInternal();
        getCurrentConnection().clearNamespaces();
    }

    @Override
    public void removeNamespaceInternal(String prefix) throws SailException {
        //super.removeNamespaceInternal(prefix);
        getCurrentConnection().removeNamespace(prefix);
    }

    @Override
    protected void clearInternal(Resource... contexts) throws SailException {
        getCurrentConnection().clear(contexts);
    }

    @Override
    protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
            throws SailException {
        return getCurrentConnection().getNamespaces();
    }

    @Override
    protected String getNamespaceInternal(String prefix) throws SailException {
        return getCurrentConnection().getNamespace(prefix);
    }

    @Override
    protected void commitInternal() throws SailException {
        super.commitInternal();
        this.connA.commit();
        this.connB.commit();
    }

    @Override
    public void flush() throws SailException {
        super.flush();
        this.connA.flush();
        this.connB.flush();
    }

    @Override
    public void flushUpdates() throws SailException {
        super.flushUpdates();
        this.connA.flush();
        this.connB.flush();
    }

    @Override
    public void startUpdate(UpdateContext op) throws SailException {
        // @todo: is this not strange that both are prepared?
        this.connA.startUpdate(op);
        this.connB.startUpdate(op);

    }

    @Override
    protected void endUpdateInternal(UpdateContext op) throws SailException {
        // @todo: is this not strange that both are prepared?
        this.connA.endUpdate(op);
        this.connB.endUpdate(op);
    }

    @Override
    protected void rollbackInternal() throws SailException {
        getCurrentConnection().rollback();
    }

    @Override
    public boolean pendingRemovals() {
        return false;
    }

    @Override
    protected void closeInternal() throws SailException {
        logger.info("Number of times native store was called:" + this.tripleSource.getCount());
        super.closeInternal();
        //this.nativeStoreConnection.close();
        this.connA.close();
        this.connB.close();
        this.connectionLock.release();
    }

    @Override
    protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
            throws SailException {
        return getCurrentConnection().getContextIDs();
    }

    @Override
    protected long sizeInternal(Resource... contexts) throws SailException {
        //return hybridStore.getNativeStoreConnection().size(contexts);
        long sizeNativeA = connA.size(contexts);
        long sizeNativeB = connB.size(contexts);
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

        logger.debug("Removing triple {} {} {}",newSubj.toString(),newPred.toString(),newObj.toString());

        // remove statement from both stores... A and B
        this.connA.removeStatement(op, newSubj, newPred, newObj, contexts);
        this.connB.removeStatement(op, newSubj, newPred, newObj, contexts);
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
                RDFWriter writer = this.hybridStore.getRdfWriterTempTriples();
                if (writer != null) {
                    writer.handleStatement(this.hybridStore.getValueFactory().createStatement(subj, pred, obj));
                } else {
                    logger.error("Writer is null!!");
                }
            }
        }
    }

    public SailConnection getCurrentConnection() {
        if (hybridStore.switchStore) {
            logger.debug("STORE B");
            return connB;
        } else {
            logger.debug("STORE A");
            return connA;
        }
    }

    public SailConnection getConnA() {
        return connA;
    }

    public SailConnection getConnB() {
        return connB;
    }

    // @todo: this logic is already present somewhere else should be moved to the store
    private long convertToId(Value iri, TripleComponentRole position) {
        return hybridStore.getHdt().getDictionary().stringToId(iri.toString(), position);
    }
}
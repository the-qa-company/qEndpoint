package eu.qanswer.hybridstore;

import eu.qanswer.model.SimpleIRIHDT;
import eu.qanswer.model.SimpleLiteralHDT;

import org.eclipse.rdf4j.IsolationLevels;
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
        super(hybridStore, hybridStore.getCurrentStore().getSailStore(), new StrictEvaluationStrategyFactory());
        this.hybridStore = hybridStore;
//    this.nativeStoreConnection = hybridStore.getConnectionNative();
        this.connA = hybridStore.getNativeStoreA().getConnection();
        this.connB = hybridStore.getNativeStoreB().getConnection();
        this.tripleSource = new HybridTripleSource(this,hybridStore.getHdt(), hybridStore);
        this.queryPreparer = new HybridQueryPreparer(hybridStore, tripleSource);
    }

    @Override
    public void begin() throws SailException {
        // lock logic is here so that the connections is blocked
        try {
            this.hybridStore.manager.waitForActiveLocks();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.connectionLock = this.hybridStore.connectionsLockManager.createLock("connection-lock");

        super.begin();

        long count = this.hybridStore.triplesCount;

        logger.info("--------------: " + count);

        // Merge only if threshold in native store exceeded and not merging with hdt
        if (count >= hybridStore.getThreshold() && !hybridStore.isMerging()) {
            logger.info("Merging..." + count);
            hybridStore.makeMerge();
        }
        //this.getCurrentConnection().begin();
        this.connA.begin(IsolationLevels.NONE);
        this.connB.begin(IsolationLevels.NONE);
    }

    private void getCardinality() {
        ParsedTupleQuery query = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL,
                "select * where {?a ?b ?c. }", null);

        TupleExpr expr = query.getTupleExpr();
        double cardinality = this.hybridStore.getCurrentStore().getSailStore().getEvaluationStatistics().getCardinality(
                expr
        );
        logger.info("Cardinality = " + cardinality);
    }

    // for SPARQL queries
    @Override
    protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
        HybridTripleSource tripleSource = queryPreparer.getTripleSource();
        // @todo: this looks dangerous ..... the connection is setting a connection in the triple source, what happens if there are multiple connections, are they overwriting each other!
        // each hybridStoreConneciton have a triple source ( ideally it should be in the query preparer as in rdf4j..)
        tripleSource.setConnA(this.connA);
        tripleSource.setConnB(this.connB);
        tripleSource.setConnCurr(getCurrentConnection());
//        CloseableIteration<? extends Statement, SailException> statements = getCurrentConnection().
//        getStatements(null, this.hybridStore.valueFactory.createIRI("http://p0"), this.hybridStore.valueFactory.createIRI("http://o0"), true, (Resource) null);
//        while (statements.hasNext()) System.out.println(statements.next());

        // TODO: check max execution time
        return queryPreparer.evaluate(tupleExpr, dataset, bindings, includeInferred, 0);
    }

    // USED from connection get api not SPARQL
    @Override
    protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
        tripleSource.setConnA(this.connA);
        tripleSource.setConnB(this.connB);
        tripleSource.setConnCurr(getCurrentConnection());
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
        IRI newPred = this.hybridStore.getIriConverter().convertPred(pred);
        Resource newSubj = this.hybridStore.getIriConverter().convertSubj(subj);
        Value newObj = this.hybridStore.getIriConverter().convertObj(obj);
        TripleID tripleID = getTripleID(newSubj, newPred, newObj);

        if (!tripleExistInHDT(tripleID)) {
            getCurrentConnection().addStatement(
                    newSubj,
                    newPred,
                    newObj,
                    contexts
            );

//            CloseableIteration<? extends Statement, SailException> statements = getCurrentConnection().
//                    getStatements(null, newPred, newObj, true, contexts);
//            while (statements.hasNext()) System.out.println(statements.next());
            // modify the bitmaps if the IRIs used are in HDT
            this.hybridStore.modifyBitmaps(this.hybridStore.getHdt(), newSubj, newPred, newObj);
            // increase the number of statements
            this.hybridStore.triplesCount++;


            // if merge is happening we don't insert the converted IRIs because they rely on the old index after merge

      /*
      if (hybridStore.isMerging()) {
        this.nativeStoreConnection.addStatement(
                subj,
                pred,
                obj,
                contexts
        );
      } else { // not merging insert with HDT IDs

      }
      */
        }
    }

    // @TODO: I think this is also not used because addStatement is used
    @Override
    public void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
            throws SailException {

        this.getCurrentConnection().addStatement(subj, pred, obj, contexts);
    }

    private boolean inOtherStore(Resource subj, IRI pred, Value obj) {
        // only in the case while merging - we check if the triple exists in the other store
        if (true) {
            System.out.println("checking in other store -========== = == = == = = = ");
            // if delta is B then check in A
            if (hybridStore.switchStore) {
                return this.connA.hasStatement(subj, pred, obj, false);
            } else
                return this.connB.hasStatement(subj, pred, obj, false);
        }
        return false;
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
        this.connectionLock.release();
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
        logger.info("Number of counts native store was called:" + this.tripleSource.getCount());
        super.closeInternal();
        //this.nativeStoreConnection.close();
        this.connA.close();
        this.connB.close();
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
        Resource newSubj = this.hybridStore.getIriConverter().convertSubj(subj);
        IRI newPred = this.hybridStore.getIriConverter().convertPred(pred);
        Value newObj;
        newObj = this.hybridStore.getIriConverter().convertObj(obj);
        // @todo: should we remove not only over the current store, I mean it will work, but it is an overhead
        // remove statement from both stores... A and B
        this.connA.removeStatement(op, newSubj, newPred, newObj, contexts);
        this.connB.removeStatement(op, newSubj, newPred, newObj, contexts);
        this.hybridStore.triplesCount--;

        TripleID tripleID = getTripleID(newSubj, newPred, newObj);
        assignBitMapDeletes(tripleID, subj, pred, obj);
    }

    // @todo: I think this is never used since it is not called in removeStatement, not sure if this is good, since there is some logic that we might miss
    @Override
    public void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... context)
            throws SailException {
        throw new SailReadOnlyException("");
    }

    private TripleID getTripleID(Resource subj, IRI pred, Value obj) {
        long subjId = -1;
        long predId = -1;
        long objId = -1;
        if (subj instanceof SimpleIRIHDT)
            subjId = ((SimpleIRIHDT) subj).getId();
        else
            subjId = convertToId(subj, TripleComponentRole.SUBJECT);
        if (pred instanceof SimpleIRIHDT)
            predId = ((SimpleIRIHDT) pred).getId();
        else
            predId = convertToId(pred, TripleComponentRole.PREDICATE);
        if (obj instanceof SimpleIRIHDT)
            objId = ((SimpleIRIHDT) obj).getId();
        else if (obj instanceof SimpleLiteralHDT)
            objId = ((SimpleLiteralHDT) obj).getHdtID();
        else
            objId = convertToId(obj, TripleComponentRole.OBJECT);

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
                if (this.hybridStore.isMerging)
                    this.hybridStore.getTempDeleteBitMap().set(index - 1, true);
            }
        } else {
            // means that the triple doesn't exist in HDT - we have to dump it while merging
            if (this.hybridStore.isMerging) {
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
        if (hybridStore.switchStore)
            return connB;
        else
            return connA;
    }

    private long convertToId(Value iri, TripleComponentRole position) {
        return hybridStore.getHdt().getDictionary().stringToId(iri.toString(), position);
    }
}

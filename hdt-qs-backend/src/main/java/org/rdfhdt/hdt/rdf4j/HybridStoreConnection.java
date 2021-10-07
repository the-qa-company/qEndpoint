package org.rdfhdt.hdt.rdf4j;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategyFactory;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.sail.*;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.rdf4j.utility.HDTConverter;
import org.rdfhdt.hdt.rdf4j.utility.IRIConverter;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HybridStoreConnection extends SailSourceConnection {

  HybridStore hybridStore;
  HDTConverter hdtConverter;
  IRIConverter iriConverter;
//  SailConnection nativeStoreConnection;
  SailConnection connA;
  SailConnection connB;
  private HybridTripleSource tripleSource;
  private HybridQueryPreparer queryPreparer;

  private static final Logger logger = LoggerFactory.getLogger(HybridStoreConnection.class);
  public HybridStoreConnection(HybridStore hybridStore) {
    super(hybridStore, hybridStore.getCurrentStore().getSailStore(),new StrictEvaluationStrategyFactory());
    this.hybridStore = hybridStore;
    this.hdtConverter = new HDTConverter(hybridStore.getHdt());
    this.iriConverter = new IRIConverter(hybridStore.getHdt());
//    this.nativeStoreConnection = hybridStore.getConnectionNative();
    this.connA = hybridStore.getNativeStoreA().getConnection();
    this.connB = hybridStore.getNativeStoreB().getConnection();
    this.tripleSource = new HybridTripleSource(hybridStore.getHdt(),hybridStore);
    this.queryPreparer = new HybridQueryPreparer(hybridStore,tripleSource);
  }

  @Override
  public void begin() throws SailException {
    super.begin();
    Stopwatch stopwatch = Stopwatch.createStarted();
    long count = this.getCurrentConnection().size();
    stopwatch.stop(); // optional
    System.out.println("Time elapsed for count request: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));

    //long count = 0;
    System.err.println("--------------: "+count);

    try {
      this.hybridStore.manager.waitForActiveLocks();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    this.lock = this.hybridStore.connectionsLockManager.createLock("connection-lock");
    // Merge only if threshold in native store exceeded and not merging with hdt
    if(count >= hybridStore.getThreshold() && !hybridStore.isMerging()){
      System.out.println("Merging..."+count);
      hybridStore.makeMerge();
    }
    //this.getCurrentConnection().begin();
    this.connA.begin();
    this.connB.begin();

  }
  // for SPARQL queries
  @Override
  protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
    HybridTripleSource tripleSource = queryPreparer.getTripleSource();
    tripleSource.setConnA(this.connA);
    tripleSource.setConnB(this.connB);
    tripleSource.setConnCurr(getCurrentConnection());
    return queryPreparer.evaluate(tupleExpr, dataset, bindings, includeInferred,0);
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
    this.getCurrentConnection().setNamespace(prefix,name);
  }

  @Override
  public void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
          throws SailException {
    this.getCurrentConnection().addStatement(subj,pred,obj,contexts);
  }

  @Override
  public boolean isActive() throws UnknownSailTransactionStateException {
    return this.getCurrentConnection().isActive();
  }

  @Override
  public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {

    if(!tripleExistInHDT(subj,pred,obj)) {
      // TODO: convert to Ids if exist, else keep
      IRI newPred = iriConverter.convertPred(pred);
      Resource newSubj = iriConverter.convertSubj(subj);
      Value newObj = null;
      if (obj instanceof Literal) {
        newObj = iriConverter.convertLiteral((Literal) obj);
      } else {
        newObj = iriConverter.convertObj(obj);
      }
      getCurrentConnection().addStatement(
              newSubj,
              newPred,
              newObj,
              contexts
      );
//      // if merge is happening we don't insert the converted IRIs because they rely on the old index after merge
//      if (hybridStore.isMerging()) {
//        this.nativeStoreConnection.addStatement(
//                subj,
//                pred,
//                obj,
//                contexts
//        );
//      } else { // not merging insert with HDT IDs
//
//      }
    }
  }

  private boolean inOtherStore(Resource subj, IRI pred, Value obj) {
    // only in the case while merging - we check if the triple exists in the other store
    if(true){
      System.out.println("checking in other store -========== = == = == = = = ");
      // if delta is B then check in A
      if(hybridStore.switchStore){
        return this.connA.hasStatement(subj,pred,obj,false);
      }else
        return this.connB.hasStatement(subj,pred,obj,false);
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
  public void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... context)
          throws SailException {
    throw new SailReadOnlyException("");
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
    //this.nativeStoreConnection.commit();
    this.connA.commit();
    this.connB.commit();
    this.lock.release();
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
  private Lock lock;
  @Override
  public void startUpdate(UpdateContext op) throws SailException {

//    try {
//      hybridStore.manager.waitForActiveLocks();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
    //this.nativeStoreConnection.startUpdate(op);
    this.connA.startUpdate(op);
    this.connB.startUpdate(op);
    // release the lock after the update is finished

  }

  @Override
  protected void endUpdateInternal(UpdateContext op) throws SailException {
    //this.nativeStoreConnection.endUpdate(op);
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
    SailConnection connectionA = this.hybridStore.getNativeStoreA().getConnection();
    long sizeNativeA = connectionA.size(contexts);
    connectionA.close();
    SailConnection connectionB = this.hybridStore.getNativeStoreB().getConnection();
    long sizeNativeB = connectionB.size(contexts);
    connectionB.close();
    long sizeHdt = this.hybridStore.getHdt().getTriples().getNumberOfElements();

    long sizeDeleted = this.hybridStore.getDeleteBitMap().countOnes();
    System.out.println("---------------------------");
    System.out.println("Size native A:"+sizeNativeA);
    System.out.println("Size native B:"+sizeNativeB);
    System.out.println("Size deleted:"+sizeDeleted);
    System.out.println("Size size HDT:"+sizeHdt);
    System.out.println("---------------------------");
    return sizeHdt + sizeNativeA + sizeNativeB - sizeDeleted;
  }

  @Override
  public void removeStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
    //printTriples();
    Resource newSubj = iriConverter.convertSubj(subj);
    IRI newPred = iriConverter.convertPred(pred);
    Value newObj;
    if(obj instanceof Literal){
      newObj = iriConverter.convertLiteral((Literal)obj);
    }else{
      newObj = iriConverter.convertObj(obj);
    }
    // remove statement from both stores... A and B
    this.connA.removeStatement(op, newSubj, newPred, newObj, contexts);
    this.connB.removeStatement(op, newSubj, newPred, newObj, contexts);

    long subjId;
    long predId;
    long objId;
    if(subj instanceof SimpleIRIHDT){
      subjId = hdtConverter.subjectId(subj);
    }else{
      subjId = convertToId(subj,TripleComponentRole.SUBJECT);
    }
    if(pred instanceof SimpleIRIHDT){
      predId = hdtConverter.predicateId(pred);
    }else{
      predId = convertToId(pred,TripleComponentRole.PREDICATE);
    }
    if(obj instanceof SimpleIRIHDT){ // TODO: or Literals
      objId = hdtConverter.objectId(obj);
    }else{
      objId = convertToId(obj,TripleComponentRole.OBJECT);
    }
    assignBitMapDeletes(subjId,predId,objId,subj,pred,obj);
  }
  private boolean tripleExistInHDT(Resource subj, IRI pred, Value obj){
    long subjId;
    long predId;
    long objId;
    if(subj instanceof SimpleIRIHDT){
      subjId = hdtConverter.subjectId(subj);
    }else{
      subjId = convertToId(subj,TripleComponentRole.SUBJECT);
    }
    if(pred instanceof SimpleIRIHDT){
      predId = hdtConverter.predicateId(pred);
    }else{
      predId = convertToId(pred,TripleComponentRole.PREDICATE);
    }
    if(obj instanceof SimpleIRIHDT){ // TODO: or Literals
      objId = hdtConverter.objectId(obj);
    }else{
      objId = convertToId(obj,TripleComponentRole.OBJECT);
    }
    TripleID t = new TripleID(subjId, predId, objId);
    Iterator<TripleID> iter = hybridStore.getHdt().getTriples().search(t);
    // if iterator is empty then the given triple 't' doesn't exist in HDT
    if(iter.hasNext()){
      TripleID next = iter.next();
      long index = next.getIndex();
      if(index != 0){
        boolean res = !this.hybridStore.getDeleteBitMap().access(index -1);
        return res;
      }
    }
    return false;
  }
  private void assignBitMapDeletes(long subjId,long predId,long objecId,Resource subj, IRI pred, Value obj) throws SailException {
    if(subjId != -1 && predId != -1 && objecId != -1) {
      TripleID t = new TripleID(subjId, predId, objecId);
      Iterator<TripleID> iter = hybridStore.getHdt().getTriples().search(t);
      long index = -1;

      if (iter.hasNext())
        index = iter.next().getIndex();
      if (index != -1) {
        this.hybridStore.getDeleteBitMap().set(index - 1, true);
        if(this.hybridStore.isMerging)
          this.hybridStore.getTempDeleteBitMap().set(index -1,true);
      }else{
        // means that the triple doesn't exist in HDT - we have to dump it while merging
        if(this.hybridStore.isMerging){
          RDFWriter writer = this.hybridStore.getRdfWriterTempTriples();
          if(writer != null) {
            writer.handleStatement(this.hybridStore.getValueFactory().createStatement(subj, pred, obj));
          }else{
            logger.error("Writer is null!!");
          }
        }
      }
    }
  }
  public SailConnection getCurrentConnection(){
    if(hybridStore.switchStore)
      return connB;
    else
      return connA;
  }
  private long convertToId(Value iri,TripleComponentRole position){
    return hybridStore.getHdt().getDictionary().stringToId(iri.toString(),position);
  }
}

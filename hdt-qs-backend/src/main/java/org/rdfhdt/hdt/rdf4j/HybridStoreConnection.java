package org.rdfhdt.hdt.rdf4j;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategyFactory;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.sail.*;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.rdf4j.utility.HDTConverter;
import org.rdfhdt.hdt.rdf4j.utility.IRIConverter;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HybridStoreConnection extends SailSourceConnection {

  HybridStore hybridStore;
  HDTConverter hdtConverter;
  IRIConverter iriConverter;
  SailConnection nativeStoreConnection;
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
    this.nativeStoreConnection = hybridStore.getConnectionNative();
    this.connA = hybridStore.getNativeStoreA().getConnection();
    this.connB = hybridStore.getNativeStoreB().getConnection();
    this.tripleSource = new HybridTripleSource(hybridStore.getHdt(),hybridStore);
    this.queryPreparer = new HybridQueryPreparer(hybridStore,tripleSource);
  }

  @Override
  public void begin() throws SailException {
    super.begin();
    long count = this.nativeStoreConnection.size();
    System.err.println("--------------: "+count);
    // Merge only if threshold in native store exceeded and not merging with hdt
    if(count >= hybridStore.getThreshold() && !hybridStore.isMerging()){
      System.out.println("Merging..."+count);
      hybridStore.makeMerge();
    }
    this.nativeStoreConnection.begin();
    this.connA.begin();
    this.connB.begin();
  }
  // for SPARQL queries
  @Override
  protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
    HybridTripleSource tripleSource = queryPreparer.getTripleSource();
    tripleSource.setConnA(this.connA);
    tripleSource.setConnB(this.connB);
    tripleSource.setConnCurr(this.nativeStoreConnection);
    return queryPreparer.evaluate(tupleExpr, dataset, bindings, includeInferred,0);
  }

  // USED from connection get api not SPARQL
  @Override
  protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
    tripleSource.setConnA(this.connA);
    tripleSource.setConnB(this.connB);
    tripleSource.setConnCurr(this.nativeStoreConnection);
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
    this.nativeStoreConnection.setNamespace(prefix,name);
  }

  @Override
  public void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
          throws SailException {
    this.nativeStoreConnection.addStatement(subj,pred,obj,contexts);
  }

  @Override
  public boolean isActive() throws UnknownSailTransactionStateException {
    return this.nativeStoreConnection.isActive();
  }

  @Override
  public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
    // TODO: convert to Ids if exist, else keep
    Value newObj;
    if(obj instanceof Literal){
      newObj = iriConverter.convertLiteral((Literal)obj);
    }else{
      newObj = iriConverter.convertObj(obj);
    }
    this.nativeStoreConnection.addStatement(
            iriConverter.convertSubj(subj),
            iriConverter.convertPred(pred),
            newObj,
            contexts
    );
  }


  @Override
  public void clearNamespacesInternal() throws SailException {
    //super.clearNamespacesInternal();
    this.nativeStoreConnection.clearNamespaces();
  }

  @Override
  public void removeNamespaceInternal(String prefix) throws SailException {
    //super.removeNamespaceInternal(prefix);
    this.nativeStoreConnection.removeNamespace(prefix);
  }
  @Override
  public void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... context)
          throws SailException {
    throw new SailReadOnlyException("");
  }

  @Override
  protected void clearInternal(Resource... contexts) throws SailException {
   this.nativeStoreConnection.clear(contexts);
  }
  @Override
  protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
          throws SailException {
    return this.nativeStoreConnection.getNamespaces();
  }

  @Override
  protected String getNamespaceInternal(String prefix) throws SailException {
    return this.nativeStoreConnection.getNamespace(prefix);
  }

  @Override
  protected void commitInternal() throws SailException {
    super.commitInternal();
    this.nativeStoreConnection.commit();
    this.connA.commit();
    this.connB.commit();
  }

  @Override
  public void flush() throws SailException {
    super.flush();
    this.nativeStoreConnection.flush();
  }

  @Override
  public void flushUpdates() throws SailException {
    super.flushUpdates();
    this.nativeStoreConnection.flush();
  }

  @Override
  public void startUpdate(UpdateContext op) throws SailException {
    this.nativeStoreConnection.startUpdate(op);
  }

  @Override
  protected void endUpdateInternal(UpdateContext op) throws SailException {
    this.nativeStoreConnection.endUpdate(op);
  }

  @Override
  protected void rollbackInternal() throws SailException {
    this.nativeStoreConnection.rollback();
  }

  @Override
  public boolean pendingRemovals() {
    return false;
  }

  @Override
  protected void closeInternal() throws SailException {
    super.closeInternal();
    this.nativeStoreConnection.close();
    this.connA.close();
    this.connB.close();
  }

  @Override
  protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
          throws SailException {
    return this.nativeStoreConnection.getContextIDs();
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
    // remove from native current store
    Resource newSubj = iriConverter.convertSubj(subj);
    IRI newPred = iriConverter.convertPred(pred);
    Value newObj;
    if(obj instanceof Literal){
      newObj = iriConverter.convertLiteral((Literal)obj);
    }else{
      newObj = iriConverter.convertObj(obj);
    }
    this.nativeStoreConnection.removeStatement(op, newSubj, newPred, newObj, contexts);
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
    assignBitMapDeletes(subjId,predId,objId);

  }
  private void assignBitMapDeletes(long subjId,long predId,long objecId) throws SailException {
    if(subjId != -1 && predId != -1 && objecId != -1) {
      TripleID t = new TripleID(subjId, predId, objecId);
      Iterator<TripleID> iter = hybridStore.getHdt().getTriples().search(t);
      long index = -1;

      if (iter.hasNext())
        index = iter.next().getIndex();
      if (index != -1)
        this.hybridStore.getDeleteBitMap().set(index - 1, true);
    }
  }
  private long convertToId(Value iri,TripleComponentRole position){
    return hybridStore.getHdt().getDictionary().stringToId(iri.toString(),position);
  }
}

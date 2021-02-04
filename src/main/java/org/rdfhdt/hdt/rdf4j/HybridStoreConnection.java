package org.rdfhdt.hdt.rdf4j;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategyFactory;
import org.eclipse.rdf4j.sail.*;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;


import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class HybridStoreConnection extends SailSourceConnection {

  HybridStore hybridStore;

  public HybridStoreConnection(HybridStore hybridStore) {
    super(hybridStore, hybridStore.getCurrentStore().getSailStore(),new StrictEvaluationStrategyFactory());
    this.hybridStore = hybridStore;
  }

  @Override
  public void begin() throws SailException {
    super.begin();
    long count = hybridStore.getNativeStoreConnection().size((Resource)null);
    System.err.println("--------------: "+count);
    // Merge only if threshold in native store exceeded and not merging with hdt
    if(count >= hybridStore.getThreshold() && !hybridStore.isMerging()){
      System.out.println("Merging...");
      hybridStore.makeMerge();
    }
    this.hybridStore.getNativeStoreConnection().begin();
  }
  // for SPARQL queries
  @Override
  protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {

    return hybridStore.getQueryPreparer().evaluate(tupleExpr, dataset, bindings, includeInferred,0);
  }

  // USED from connection get api not SPARQL
  @Override
  protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
    CloseableIteration<? extends Statement, QueryEvaluationException> result =
            hybridStore.getTripleSource().getStatements(subj, pred, obj, contexts);
    return new ExceptionConvertingIteration<Statement, SailException>(result) {
      @Override
      protected SailException convert(Exception e) {
        return new SailException(e);
      }
    };
  }
  @Override
  public void setNamespaceInternal(String prefix, String name) throws SailException {
    super.setNamespaceInternal(prefix,name);
    hybridStore.getNativeStoreConnection().setNamespace(prefix,name);
  }

  @Override
  public void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
          throws SailException {
    hybridStore.getNativeStoreConnection().addStatement(subj,pred,obj,contexts);
  }

  @Override
  public boolean isActive() throws UnknownSailTransactionStateException {
    return hybridStore.getNativeStoreConnection().isActive();
  }

  @Override
  public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
    hybridStore.getNativeStoreConnection().addStatement(subj,pred,obj,contexts);
  }

  @Override
  public void clearNamespacesInternal() throws SailException {
    throw new SailReadOnlyException("");
  }

  @Override
  public void removeNamespaceInternal(String prefix) throws SailException {
    throw new SailReadOnlyException("");
  }
  @Override
  public void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... context)
          throws SailException {
    throw new SailReadOnlyException("");
  }

  @Override
  protected void clearInternal(Resource... contexts) throws SailException {
    hybridStore.getNativeStoreConnection().clear(contexts);
  }

  @Override
  protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
          throws SailException {
    return hybridStore.getNativeStoreConnection().getNamespaces();
  }

  @Override
  protected String getNamespaceInternal(String prefix) throws SailException {
    return hybridStore.getNativeStoreConnection().getNamespace(prefix);
  }

  @Override
  protected void commitInternal() throws SailException {
    super.commitInternal();
    hybridStore.getNativeStoreConnection().commit();
  }

  @Override
  public void startUpdate(UpdateContext op) throws SailException {
    hybridStore.getNativeStoreConnection().startUpdate(op);
  }

  @Override
  protected void endUpdateInternal(UpdateContext op) throws SailException {
    hybridStore.getNativeStoreConnection().endUpdate(op);
  }

  @Override
  protected void rollbackInternal() throws SailException {
    hybridStore.getNativeStoreConnection().rollback();
  }

  @Override
  public boolean pendingRemovals() {
    return false;
  }

  @Override
  protected void closeInternal() throws SailException {

  }

  @Override
  protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
          throws SailException {
    return null;
  }

  @Override
  protected long sizeInternal(Resource... contexts) throws SailException {
    //return hybridStore.getNativeStoreConnection().size(contexts);
    long sizeNativeA = this.hybridStore.getNativeStoreA().getConnection().size(contexts);
    long sizeNativeB = this.hybridStore.getNativeStoreB().getConnection().size(contexts);
    long sizeHdt = this.hybridStore.getHdt().getTriples().getNumberOfElements();

    long sizeDeleted = this.hybridStore.getDeleteBitMap().countOnes();

    return sizeHdt + sizeNativeA + sizeNativeB - sizeDeleted;
  }

  @Override
  public void removeStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
    // remove from native current store
    Stopwatch stopwatch = Stopwatch.createStarted();
    this.hybridStore.getNativeStoreConnection().removeStatement(op, subj, pred, obj, contexts);
    stopwatch.stop(); // optional
    System.out.println("Time elapsed to delete from  native store: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
    //assignBitSets(subj,pred,obj);
    // add to delete store so we can skip it if it exists in hdt
    long subjId = convertToId(subj,TripleComponentRole.SUBJECT);
    //IRI s = this.hybridStore.getValueFactory().createIRI("http://hdt-"+subjId);
    long predId = convertToId(pred,TripleComponentRole.PREDICATE);
    //IRI p= this.hybridStore.getValueFactory().createIRI("http://hdt-"+predId);
    long objId = convertToId(obj,TripleComponentRole.OBJECT);
    //IRI o = this.hybridStore.getValueFactory().createIRI("http://hdt-"+objId);

    stopwatch = Stopwatch.createStarted();
    assignBitMapDeletes(subjId,predId,objId);
    stopwatch.stop(); // optional
    System.out.println("Time elapsed to delete from hdt: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }
  private void assignBitMapDeletes(long subjId,long predId,long objecId) throws SailException {
    TripleID t = new TripleID(subjId, predId, objecId);
    Iterator<TripleID> iter = hybridStore.getHdt().getTriples().search(t);
    long index = -1;

    if(iter.hasNext())
      index = iter.next().getIndex();
    if(index != -1)
      this.hybridStore.getDeleteBitMap().set(index-1,true);
    else{
      //System.out.println("triple not found in HDT to be deleted");
    }
  }
  private long convertToId(Value iri,TripleComponentRole position){
    return hybridStore.getHdt().getDictionary().stringToId(iri.toString(),position);
  }
}

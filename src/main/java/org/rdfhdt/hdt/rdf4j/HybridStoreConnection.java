package org.rdfhdt.hdt.rdf4j;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import eu.qanswer.enpoint.Sparql;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategyFactory;
import org.eclipse.rdf4j.sail.*;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.rdf4j.utility.HDTConverter;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class HybridStoreConnection extends SailSourceConnection {

  HybridStore hybridStore;
  HDTConverter hdtConverter;
  private static final Logger logger = LoggerFactory.getLogger(HybridStoreConnection.class);
  public HybridStoreConnection(HybridStore hybridStore) {
    super(hybridStore, hybridStore.getCurrentStore().getSailStore(),new StrictEvaluationStrategyFactory());
    this.hybridStore = hybridStore;
    this.hdtConverter = new HDTConverter(hybridStore.getHdt());
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
    // TODO: convert to Ids if exist, else keep
    // use
    String subjStr = subj.toString();
    Resource newSubj = null;
    long subjId = this.hybridStore.getHdt().getDictionary().stringToId(subjStr,TripleComponentRole.SUBJECT);
    if(subjId != -1){
      if(subjId <= this.hybridStore.getHdt().getDictionary().getNshared()){
        newSubj = this.hybridStore.getValueFactory().createIRI("http://hdt.org/SO" + subjId);
      }else {
        newSubj = this.hybridStore.getValueFactory().createIRI("http://hdt.org/S" + subjId);
      }
    }else{
      newSubj = subj;
    }
    String predStr = pred.toString();
    IRI newPred = null;
    long predId = this.hybridStore.getHdt().getDictionary().stringToId(predStr,TripleComponentRole.PREDICATE);
    if(predId != -1){
      newPred = this.hybridStore.getValueFactory().createIRI("http://hdt.org/P"+predId);
    }else{
      newPred = pred;
    }
    String objStr = pred.toString();
    Value newObj = null;
    long objId = this.hybridStore.getHdt().getDictionary().stringToId(objStr,TripleComponentRole.OBJECT);
    if(objId != -1){
      newObj = this.hybridStore.getValueFactory().createIRI("http://hdt.org/O"+objId);
    }else{
      newObj = obj;
    }

    hybridStore.getNativeStoreConnection().addStatement(newSubj,newPred,newObj,contexts);
      //hybridStore.getNativeStoreConnection().addStatement(subj,pred,obj,contexts);

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
    this.hybridStore.getNativeStoreConnection().removeStatement(op, subj, pred, obj, contexts);

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
    if(obj instanceof SimpleIRIHDT){
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

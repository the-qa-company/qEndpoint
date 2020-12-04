package org.rdfhdt.hdt.rdf4j;

import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategyFactory;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.SailReadOnlyException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.rdf4j.misc.HDTQueryPreparer;

public class HybridStoreConnection extends SailSourceConnection {

  private final HDTQueryPreparer queryPreparer;
  HybridStore hybridStore;
  // QueryPreparer queryPreparer;
  public HybridStoreConnection(HybridStore hybridStore) {
    super(hybridStore, hybridStore.getCurrentStore().getSailStore(),new StrictEvaluationStrategyFactory());
    this.hybridStore = hybridStore;
    this.queryPreparer = new HDTQueryPreparer(hybridStore.getTripleSource());
  }

  @Override
  public void begin() throws SailException {
    this.hybridStore.getNativeStoreConnection().begin();
  }

  @Override
  public void startUpdate(UpdateContext op) throws SailException {
    hybridStore.getNativeStoreConnection().startUpdate(op);
  }


  @Override
  public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
    System.out.println("--------------: "+hybridStore.getCurrentCount());
    //getCount();
    hybridStore.getNativeStoreConnection().addStatement(op, subj, pred, obj, contexts);
  }
  void getCount(){
    CloseableIteration<? extends Statement, SailException>  res =
            hybridStore.getCurrentStore().getConnection().getStatements(null,null,
                    null,false,null);
    int count = 0;
    while (res.hasNext()){
      res.next();
      count++;
    }
    System.out.println("count:============"+count);
  }

  @Override
  protected void endUpdateInternal(UpdateContext op) throws SailException {
    hybridStore.getNativeStoreConnection().endUpdate(op);
  }

  @Override
  protected IsolationLevel getTransactionIsolation() {
    return this.hybridStore.getCurrentStore().getDefaultIsolationLevel();
  }

  @Override
  public boolean isActive() throws UnknownSailTransactionStateException {
    return this.hybridStore.getNativeStoreConnection().isActive();
  }


  @Override
  protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(
          Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts)
          throws SailException {
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
    hybridStore.getNativeStoreConnection().setNamespace(prefix,name);
  }

  @Override
  public void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
          throws SailException {
    System.out.println("--------------: "+hybridStore.getCurrentCount());
    hybridStore.getNativeStoreConnection().addStatement(subj,pred,obj,contexts);
  }


  @Override
  protected void startTransactionInternal() throws SailException {
    new SailReadOnlyException("");
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
    hybridStore.getNativeStoreConnection().commit();
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
  protected void closeInternal() throws SailException {}

  @Override
  protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
          throws SailException {
    return null;
  }

  @Override
  protected long sizeInternal(Resource... contexts) throws SailException {
    return 0;
  }
}

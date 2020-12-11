package org.rdfhdt.hdt.rdf4j;

import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategyFactory;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.SailReadOnlyException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.rdf4j.misc.HDTQueryPreparer;

public class HybridStoreConnection extends SailSourceConnection {

  HybridStore hybridStore;

  public HybridStoreConnection(HybridStore hybridStore) {
    super(hybridStore, hybridStore.getCurrentStore().getSailStore(),new StrictEvaluationStrategyFactory());
    this.hybridStore = hybridStore;
  }

  @Override
  protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {

    return hybridStore.getQueryPreparer().evaluate(tupleExpr, dataset, bindings, includeInferred,0);
  }

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
  public void begin() throws SailException {
    super.begin();
    int count = hybridStore.getCurrentCount();
    System.out.println("--------------: "+count);
    if(count >= hybridStore.getThreshold()){ // THRESHOLD
      hybridStore.makeMerge();
    }
    this.hybridStore.getNativeStoreConnection().begin();
  }
  @Override
  public void setNamespaceInternal(String prefix, String name) throws SailException {
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

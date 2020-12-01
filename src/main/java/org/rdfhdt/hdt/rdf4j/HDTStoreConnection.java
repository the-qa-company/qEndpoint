package org.rdfhdt.hdt.rdf4j;

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
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.SailReadOnlyException;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;

public class HDTStoreConnection extends SailSourceConnection {

  HDTStore hdtStore;
  // QueryPreparer queryPreparer;

  public HDTStoreConnection(HDTStore hdtStore) {
    super(hdtStore, new HDTSailStore(hdtStore.getHdt()), new StrictEvaluationStrategyFactory());
    this.hdtStore = hdtStore;
    // this.queryPreparer = new HDTQueryPreparer(hdtSail.getTripleSource());
  }

  @Override
  public void setNamespaceInternal(String prefix, String name) throws SailException {
    throw new SailReadOnlyException("");
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
  public void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
      throws SailException {
    throw new SailReadOnlyException("");
  }

  @Override
  public void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... context)
      throws SailException {
    throw new SailReadOnlyException("");
  }

  @Override
  protected void clearInternal(Resource... contexts) throws SailException {
    throw new SailReadOnlyException("");
  }

  @Override
  protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
      throws SailException {
    return null;
  }

  @Override
  protected String getNamespaceInternal(String prefix) throws SailException {
    return null;
  }

  @Override
  protected void commitInternal() throws SailException {
    new SailReadOnlyException("");
  }

  @Override
  protected void rollbackInternal() throws SailException {
    new SailReadOnlyException("");
  }

  @Override
  protected void startTransactionInternal() throws SailException {
    new SailReadOnlyException("");
  }

  @Override
  public boolean pendingRemovals() {
    return false;
  }

  @Override
  protected void closeInternal() throws SailException {}

  @Override
  protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(
      TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred)
      throws SailException {
    return hdtStore
        .getQueryPreparer()
        .evaluate(tupleExpr, dataset, bindings, includeInferred, 1000000);
  }

  @Override
  protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
      throws SailException {
    return null;
  }

  @Override
  protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(
      Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts)
      throws SailException {
    CloseableIteration<? extends Statement, QueryEvaluationException> result =
        hdtStore.getTripleSource().getStatements(subj, pred, obj, contexts);
    return new ExceptionConvertingIteration<Statement, SailException>(result) {
      @Override
      protected SailException convert(Exception e) {
        return new SailException(e);
      }
    };
  }

  @Override
  protected long sizeInternal(Resource... contexts) throws SailException {
    return 0;
  }
}

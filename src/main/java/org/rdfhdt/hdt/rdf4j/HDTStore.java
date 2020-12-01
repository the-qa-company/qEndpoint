package org.rdfhdt.hdt.rdf4j;

import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailChangedListener;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractSail;
import org.rdfhdt.hdt.hdt.HDT;

import java.io.File;
import java.util.List;

public class HDTStore extends AbstractSail implements NotifyingSail {

  private File file;
  private HDT hdt;
  private HDTTripleSource tripleSource;
  private HDTQueryPreparer queryPreparer;

  public HDTStore(HDT hdt) {
    this.hdt = hdt;
  }

  @Override
  public void initializeInternal() throws SailException {
    tripleSource = new HDTTripleSource(hdt);
    queryPreparer = new HDTQueryPreparer(tripleSource);
  }

  @Override
  protected void shutDownInternal() throws SailException {}

  @Override
  public boolean isWritable() throws SailException {
    return false;
  }

  @Override
  public NotifyingSailConnection getConnection() throws SailException {
    return new HDTStoreConnection(this);
  }

  @Override
  protected SailConnection getConnectionInternal() throws SailException {
    return new HDTStoreConnection(this);
  }

  @Override
  public void addSailChangedListener(SailChangedListener listener) {}

  @Override
  public void removeSailChangedListener(SailChangedListener listener) {}

  @Override
  public ValueFactory getValueFactory() {
    return SimpleValueFactory.getInstance();
  }

  @Override
  public List<IsolationLevel> getSupportedIsolationLevels() {
    return null;
  }

  @Override
  public IsolationLevel getDefaultIsolationLevel() {
    return null;
  }

  public HDTTripleSource getTripleSource() {
    return tripleSource;
  }

  public HDT getHdt() {
    return hdt;
  }

  public void setHdt(HDT hdt) {
    this.hdt = hdt;
  }

  public HDTQueryPreparer getQueryPreparer() {
    return queryPreparer;
  }

  public void setQueryPreparer(HDTQueryPreparer queryPreparer) {
    this.queryPreparer = queryPreparer;
  }

  public void setTripleSource(HDTTripleSource tripleSource) {
    this.tripleSource = tripleSource;
  }
}

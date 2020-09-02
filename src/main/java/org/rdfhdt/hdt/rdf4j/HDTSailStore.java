package org.rdfhdt.hdt.rdf4j;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailSource;
import org.eclipse.rdf4j.sail.base.SailStore;
import org.rdfhdt.hdt.hdt.HDT;

public class HDTSailStore implements SailStore {

  private HDT hdt;

  HDTSailStore(HDT hdt) {
    this.hdt = hdt;
  }

  @Override
  public ValueFactory getValueFactory() {
    return SimpleValueFactory.getInstance();
  }

  @Override
  public EvaluationStatistics getEvaluationStatistics() {
    return new HDTEvaluationStatisticsV2(hdt);
  }

  @Override
  public SailSource getExplicitSailSource() {
    return null;
  }

  @Override
  public SailSource getInferredSailSource() {
    return null;
  }

  @Override
  public void close() throws SailException {}
}

package org.rdfhdt.hdt.rdf4j.misc;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailSource;
import org.eclipse.rdf4j.sail.base.SailStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.rdf4j.CombinedEvaluationStatistics;
import org.rdfhdt.hdt.rdf4j.HDTEvaluationStatisticsV2;

public class HDTSailStore implements SailStore {

  private HDT hdt;
  NativeStore nativeStore;

  HDTSailStore(HDT hdt,NativeStore nativeStore) {
    this.hdt = hdt;
    this.nativeStore = nativeStore;
  }

  @Override
  public ValueFactory getValueFactory() {
    return SimpleValueFactory.getInstance();
  }

  @Override
  public EvaluationStatistics getEvaluationStatistics() {
    return new CombinedEvaluationStatistics(new HDTEvaluationStatisticsV2(hdt),nativeStore.getSailStore().getEvaluationStatistics());
  }

  @Override
  public SailSource getExplicitSailSource() {
    return nativeStore.getSailStore().getExplicitSailSource();
  }

  @Override
  public SailSource getInferredSailSource() {
    return nativeStore.getSailStore().getInferredSailSource();
  }

  @Override
  public void close() throws SailException {}
}

package org.rdfhdt.hdt.rdf4j;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.AbstractValueFactoryHDT;
import org.eclipse.rdf4j.model.impl.SimpleIRI;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.eclipse.rdf4j.model.impl.SimpleLiteralHDT;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.sail.SailException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.rdf4j.utility.BinarySearch;
import org.rdfhdt.hdt.rdf4j.utility.CombinedNativeStoreResult;
import org.rdfhdt.hdt.rdf4j.utility.HDTConverter;
import org.rdfhdt.hdt.rdf4j.utility.TripleWithDeleteIter;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class HybridTripleSource implements TripleSource {
  private static final Logger logger = LoggerFactory.getLogger(HybridTripleSource.class);
  private final HybridStore hybridStore;
  HDT hdt;
  HDTDictionaryMapping hdtDictionaryMapping;
  ValueFactory factory;
  long startLiteral;
  long endLiteral;
  HDTConverter hdtConverter;

  public HybridTripleSource(HDT hdt, HybridStore hybridStore) {
    this.hybridStore = hybridStore;
    this.hdt = hdt;
    this.hdtDictionaryMapping = new HDTDictionaryMapping(hdt);
    this.factory = new AbstractValueFactoryHDT(hdt);
    this.startLiteral =
        BinarySearch.first(
            hdt.getDictionary(),
            hdt.getDictionary().getNshared() + 1,
            hdt.getDictionary().getNobjects(),
            "\"");
    this.endLiteral =
        BinarySearch.last(
            hdt.getDictionary(),
            hdt.getDictionary().getNshared() + 1,
            hdt.getDictionary().getNobjects(),
            hdt.getDictionary().getNobjects(),
            "\"");
    this.hdtConverter = new HDTConverter(hdt);
  }


  @Override
  public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
      Resource resource, IRI iri, Value value, Resource... resources)
      throws QueryEvaluationException {

    CloseableIteration<? extends Statement, SailException> repositoryResult = null;

    if(hybridStore.isMerging()){
      // query both native stores
      CloseableIteration<? extends Statement, SailException> repositoryResult1 =
              this.hybridStore.getNativeStoreA().getConnection().getStatements(
                      resource,iri,value,false,resources
              );
      CloseableIteration<? extends Statement, SailException> repositoryResult2 =
              this.hybridStore.getNativeStoreB().getConnection().getStatements(
                      resource,iri,value,false,resources
              );
      repositoryResult = new CombinedNativeStoreResult(repositoryResult1,repositoryResult2);

    }else{
      repositoryResult = this.hybridStore.getCurrentStore().getConnection().getStatements(
              resource,iri,value,false,resources
      );
    }
    long subject = hdtConverter.subjectId(resource);
    long predicate = hdtConverter.predicateId(iri);
    long object = hdtConverter.objectId(value);
    if (logger.isDebugEnabled()) {
      if (resource != null) {
        logger.debug(resource.toString());
      }
      if (iri != null) {
        logger.debug(iri.toString());
      }
      if (value != null) {
        logger.debug(value.stringValue());
      }
      logger.debug(subject+"--"+predicate+"--"+object);
    }


    TripleID t = new TripleID(subject, predicate, object);
    IteratorTripleID iterator = hdt.getTriples().search(t);
    TripleWithDeleteIter tripleWithDeleteIter = new TripleWithDeleteIter(this,iterator,repositoryResult);
    return new CloseableIteration<Statement, QueryEvaluationException>() {
      @Override
      public void close() throws QueryEvaluationException {
      }

      @Override
      public boolean hasNext() throws QueryEvaluationException {
        return tripleWithDeleteIter.hasNext();
      }

      @Override
      public Statement next() throws QueryEvaluationException {
        Statement stm = tripleWithDeleteIter.next();
        return stm;
      } ///

      @Override
      public void remove() throws QueryEvaluationException {
        iterator.remove();
      }
    };
  }

  @Override
  public ValueFactory getValueFactory() {
    return factory;
  }

  public HDT getHdt() {
    return hdt;
  }

  public long getStartLiteral() {
    return startLiteral;
  }

  public long getEndLiteral() {
    return endLiteral;
  }

  public HybridStore getHybridStore() {
    return hybridStore;
  }

}

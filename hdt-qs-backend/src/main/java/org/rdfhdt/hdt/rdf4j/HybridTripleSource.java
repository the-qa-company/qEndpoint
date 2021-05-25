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
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.rdf4j.utility.*;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.impl.EmptyTriplesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
  public long startBlank;
  public long endBlank;
  HDTConverter hdtConverter;
  IRIConverter iriConverter;
  ValueFactory tempFactory;
  private SailConnection connA;
  private SailConnection connB;
  private SailConnection connCurr;
  private long numberOfCurrentTriples;
  public HybridTripleSource(HDT hdt, HybridStore hybridStore) {
    this.hybridStore = hybridStore;
    this.hdt = hdt;
    this.hdtDictionaryMapping = new HDTDictionaryMapping(hdt);
    this.factory = new AbstractValueFactoryHDT(hdt);
    this.startLiteral = hybridStore.getHdtProps().getStartLiteral();
    this.endLiteral = hybridStore.getHdtProps().getEndLiteral();
    this.startBlank = hybridStore.getHdtProps().getStartBlank();
    this.endBlank = hybridStore.getHdtProps().getEndBlank();
    this.numberOfCurrentTriples = hdt.getTriples().getNumberOfElements();
    this.hdtConverter = new HDTConverter(hdt);
    this.iriConverter = new IRIConverter(hdt);
    this.tempFactory = new MemValueFactory();

  }

  public ValueFactory getTempFactory() {
    return tempFactory;
  }
  private void initHDTIndex(){
    this.hdt = this.hybridStore.getHdt();
    this.startLiteral = hybridStore.getHdtProps().getStartLiteral();
    this.endLiteral = hybridStore.getHdtProps().getEndLiteral();
    this.startBlank = hybridStore.getHdtProps().getStartBlank();
    this.endBlank = hybridStore.getHdtProps().getEndBlank();
    this.numberOfCurrentTriples = hdt.getTriples().getNumberOfElements();
  }
  @Override
  public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
      Resource resource, IRI iri, Value value, Resource... resources)
      throws QueryEvaluationException {

    // check if the index changed
    if(numberOfCurrentTriples != this.hybridStore.getHdt().getTriples().getNumberOfElements()){
      initHDTIndex();
    }

    CloseableIteration<? extends Statement, SailException> repositoryResult = null;

    Resource newRes = iriConverter.convertSubj(resource);
    IRI newIRI = iriConverter.convertPred(iri);
    Value newValue = iriConverter.convertObj(value);


    ArrayList<SailConnection> connections = new ArrayList();
    connections.add(connA);
    connections.add(connB);

    if(hybridStore.isMerging()){
      // query both native stores
      CloseableIteration<? extends Statement, SailException> repositoryResult1 =
              connA.getStatements(
                      newRes,newIRI,newValue,false,resources
              );
      CloseableIteration<? extends Statement, SailException> repositoryResult2 =
              connB.getStatements(
                      newRes,newIRI,newValue,false,resources
              );
      repositoryResult = new CombinedNativeStoreResult(repositoryResult1,repositoryResult2);

    }else{
      repositoryResult = this.connCurr.getStatements(
              newRes, newIRI, newValue, false, resources
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

    IteratorTripleID iterator;
    //System.out.println(subject+"--"+predicate+"--"+object);
    if(subject != -1 && predicate != -1 && object != -1) {
      TripleID t = new TripleID(subject, predicate, object);
      iterator = hdt.getTriples().search(t);

    }else{ // no need to search over hdt
      iterator = new EmptyTriplesIterator(TripleComponentOrder.SPO);
    }
    TripleWithDeleteIter tripleWithDeleteIter = new TripleWithDeleteIter(this,iterator,repositoryResult,connections);
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
        tripleWithDeleteIter.remove();
      }
    };
  }

  public void setHdt(HDT hdt) {
    this.hdt = hdt;
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

  public SailConnection getConnA() {
    return connA;
  }

  public SailConnection getConnB() {
    return connB;
  }

  public void setConnA(SailConnection connA) {
    this.connA = connA;
  }

  public void setConnB(SailConnection connB) {
    this.connB = connB;
  }

  public void setConnCurr(SailConnection connCurr) {
    this.connCurr = connCurr;
  }
}

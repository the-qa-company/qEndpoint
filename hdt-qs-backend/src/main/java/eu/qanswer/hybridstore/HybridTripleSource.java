package eu.qanswer.hybridstore;

import eu.qanswer.model.AbstractValueFactoryHDT;
import eu.qanswer.model.SimpleIRIHDT;
import eu.qanswer.utils.CombinedNativeStoreResult;
import eu.qanswer.utils.HDTConverter;
import eu.qanswer.utils.IRIConverter;
import eu.qanswer.utils.TripleWithDeleteIter;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.*;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.impl.EmptyTriplesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class HybridTripleSource implements TripleSource {
  private static final Logger logger = LoggerFactory.getLogger(HybridTripleSource.class);
  private final HybridStore hybridStore;
  HDT hdt;
  ValueFactory factory;
  long startLiteral;
  long endLiteral;
  public long startBlankObjects;
  public long endBlankObjects;
  public long startBlankShared;
  public long endBlankShared;

  HDTConverter hdtConverter;
  IRIConverter iriConverter;
  ValueFactory tempFactory;
  private SailConnection connA;
  private SailConnection connB;
  private SailConnection connCurr;
  private long numberOfCurrentTriples;
  private long count = 0;
  public HybridTripleSource(HDT hdt, HybridStore hybridStore) {
    this.hybridStore = hybridStore;
    this.hdt = hdt;
    this.factory = new AbstractValueFactoryHDT(hdt);
    this.startLiteral = hybridStore.getHdtProps().getStartLiteral();
    this.endLiteral = hybridStore.getHdtProps().getEndLiteral();
    this.startBlankObjects = hybridStore.getHdtProps().getStartBlankObjects();
    this.endBlankObjects = hybridStore.getHdtProps().getEndBlankObjects();
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
    this.startBlankObjects = hybridStore.getHdtProps().getStartBlankObjects();
    this.endBlankObjects = hybridStore.getHdtProps().getEndBlankObjects();
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


    ArrayList<SailConnection> connections = new ArrayList();
    connections.add(connA);
    connections.add(connB);


    Resource newRes = iriConverter.convertSubj(resource);
    IRI newIRI = iriConverter.convertPred(iri);
    Value newValue = iriConverter.convertObj(value);

    if(shouldSearchOverNativeStore(newRes,newIRI,newValue)) {
      count++;
      if (hybridStore.isMerging()) {
        // query both native stores
        CloseableIteration<? extends Statement, SailException> repositoryResult1 =
                connA.getStatements(
                        newRes, newIRI, newValue, false, resources
                );
        CloseableIteration<? extends Statement, SailException> repositoryResult2 =
                connB.getStatements(
                        newRes, newIRI, newValue, false, resources
                );
        repositoryResult = new CombinedNativeStoreResult(repositoryResult1, repositoryResult2);

      } else {
        repositoryResult = this.connCurr.getStatements(
                newRes, newIRI, newValue, false, resources
        );
      }
    }
//    long subject = -1;
//    if(newRes == null){
//      subject = 0;
//    }else if(newRes instanceof SimpleIRIHDT){
//      subject = ((SimpleIRIHDT)newRes).getId();
//    }
//    long predicate = -1;
//    if(newIRI == null){
//      predicate = 0;
//    }else if (newIRI instanceof SimpleIRIHDT){
//      predicate = ((SimpleIRIHDT)newIRI).getId();
//    }
//    long object = -1;
//    if(newValue == null){
//      object = 0;
//    }else if(newValue instanceof SimpleIRIHDT){
//      object = ((SimpleIRIHDT)newValue).getId();
//    }


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
      // search with the ID to check if the triples has been deleted
      iterator = hdt.getTriples().searchWithId(t);

    }else{ // no need to search over hdt
      iterator = new EmptyTriplesIterator(TripleComponentOrder.SPO);
    }
//    IteratorTripleID iterator = new EmptyTriplesIterator(TripleComponentOrder.SPO);
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

  private boolean shouldSearchOverNativeStore(Resource subject,IRI predicate,Value object){
    boolean containsSubject = false;
    boolean containsPredicate = false;
    boolean containsObject = false;

    if(subject instanceof SimpleIRIHDT){
      if(((SimpleIRIHDT) subject).getId() != -1)
        containsSubject = this.hybridStore.getBitX().access(((SimpleIRIHDT) subject).getId() - 1);
      else
        containsSubject = true;
    }else {
      containsSubject = true;
    }
    if(predicate instanceof SimpleIRIHDT){
      if(((SimpleIRIHDT) predicate).getId() != -1)
        containsPredicate = this.hybridStore.getBitY().access(((SimpleIRIHDT) predicate).getId() - 1);
      else
        containsPredicate = true;
    }else {
      containsPredicate = true;
    }
    if(object instanceof SimpleIRIHDT){
      if(((SimpleIRIHDT) object).getId() != -1) {
        if(((SimpleIRIHDT) object).getPostion() == SimpleIRIHDT.SHARED_POS)
          containsObject = this.hybridStore.getBitX().access(((SimpleIRIHDT) object).getId() - 1);
        else if(((SimpleIRIHDT) object).getPostion() == SimpleIRIHDT.OBJECT_POS)
          containsObject = this.hybridStore.getBitZ().access(((SimpleIRIHDT) object).getId() - 1);
      }else
        containsObject = true;
    }else{
      containsObject = true;
    }
    return containsSubject && containsPredicate && containsObject;
  }
  /*
  private boolean shouldSearchOverNativeStore(Resource subject,IRI predicate,Value object){
    boolean containsSubject = false;
    boolean containsPredicate = false;
    boolean containsObject = false;

    if(subject instanceof SimpleIRIHDT){
      containsSubject = this.hybridStore.nativeStoreDictionary.contains(((SimpleIRIHDT) subject).getIriString());
    }
    if(predicate instanceof SimpleIRIHDT){
      containsPredicate = this.hybridStore.nativeStoreDictionary.contains(((SimpleIRIHDT) predicate).getIriString());
    }
    if(object instanceof SimpleIRIHDT){
      containsObject = this.hybridStore.nativeStoreDictionary.contains(((SimpleIRIHDT) object).getIriString());
    }
    return containsSubject && containsPredicate && containsObject;
  } */
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

  public long getCount() {
    return count;
  }
}

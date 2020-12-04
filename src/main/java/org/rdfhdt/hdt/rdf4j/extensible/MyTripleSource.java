package org.rdfhdt.hdt.rdf4j.extensible;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.AbstractValueFactoryHDT;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.eclipse.rdf4j.model.impl.SimpleLiteralHDT;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.rdf4j.HDTDictionaryMapping;
import org.rdfhdt.hdt.rdf4j.utility.BinarySearch;
import org.rdfhdt.hdt.rdf4j.utility.HDTConverter;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTripleSource implements org.eclipse.rdf4j.query.algebra.evaluation.TripleSource {
  private static final Logger logger = LoggerFactory.getLogger(MyTripleSource.class);
  private final NativeStore nativeStore;
  HDT hdt;
  HDTDictionaryMapping hdtDictionaryMapping;
  ValueFactory factory;
  long startLiteral;
  long endLiteral;
  HDTConverter hdtConverter;

  public MyTripleSource(HDT hdt, NativeStore nativeStore) {
    this.nativeStore = nativeStore;
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
    CloseableIteration<? extends Statement, SailException> repositoryResult =
            this.nativeStore.getConnection().getStatements(resource,iri,value,false,resources);
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

    return new CloseableIteration<Statement, QueryEvaluationException>() {
      @Override
      public void close() throws QueryEvaluationException {}

      @Override
      public boolean hasNext() throws QueryEvaluationException {
        boolean result = iterator.hasNext() || repositoryResult.hasNext();
        return result;
      }

      @Override
      public Statement next() throws QueryEvaluationException {
        if(iterator.hasNext()) {
          TripleID tripleID = iterator.next();
          return new Statement() {
            @Override
            public Resource getSubject() {
              if (tripleID.getSubject() <= hdt.getDictionary().getNshared()) {
                return new SimpleIRIHDT(hdt, "hdt:SO" + tripleID.getSubject());
              } else {
                return new SimpleIRIHDT(hdt, "hdt:S" + tripleID.getSubject());
              }
            }

            @Override
            public IRI getPredicate() {
              return new SimpleIRIHDT(hdt, "hdt:P" + tripleID.getPredicate());
            }

            @Override
            public Value getObject() {
              if (tripleID.getObject() >= startLiteral && tripleID.getObject() <= endLiteral) {
                // System.out.println("Literal "+tripleID.getObject()+" --
                // "+hdt.getDictionary().idToString(tripleID.getObject(),TripleComponentRole.OBJECT));
                return new SimpleLiteralHDT(hdt, tripleID.getObject(), factory);
              } else {
                // System.out.println("IRI
                // "+hdt.getDictionary().idToString(tripleID.getObject(),TripleComponentRole.OBJECT));
                if (tripleID.getObject() <= hdt.getDictionary().getNshared()) {
                  return new SimpleIRIHDT(hdt, ("hdt:SO" + tripleID.getObject()));
                } else {
                  return new SimpleIRIHDT(hdt, ("hdt:O" + tripleID.getObject()));
                }
              }
            }

            @Override
            public Resource getContext() {
              return null;
            }
          };
        }else{
          if(repositoryResult.hasNext()){
            return repositoryResult.next();
          }
          return null;
        }
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
}

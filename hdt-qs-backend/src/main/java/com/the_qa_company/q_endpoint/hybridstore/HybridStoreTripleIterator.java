package com.the_qa_company.q_endpoint.hybridstore;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.SailException;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class HybridStoreTripleIterator implements Iterator<Statement> {
    private static final Logger logger = LoggerFactory.getLogger(HybridStoreTripleIterator.class);

    private final HybridStore hybridStore;
    private final HybridTripleSource hybridTripleSource;
    private final IteratorTripleID iterator;
    private final CloseableIteration<? extends Statement, SailException> repositoryResult;

    public HybridStoreTripleIterator(HybridStore hybridStore, HybridTripleSource hybridTripleSource, IteratorTripleID iter,
                                     CloseableIteration<? extends Statement,
                                        SailException> repositoryResult
    ) {
        this.hybridStore = hybridStore;
        this.hybridTripleSource = hybridTripleSource;
        this.iterator = iter;
        this.repositoryResult = repositoryResult;
    }

    Statement next;

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        // iterate over the result of hdt
        if (iterator != null) {
            while (iterator.hasNext()) {
                TripleID tripleID = iterator.next();
                long index = iterator.getLastTriplePosition();
                if (!hybridStore.getDeleteBitMap().access(index)) {
                    Resource subject = hybridStore.getHdtConverter().IdToSubjectHDTResource(tripleID.getSubject());
                    IRI predicate = hybridStore.getHdtConverter().IdToPredicateHDTResource(tripleID.getPredicate());
                    Value object = hybridStore.getHdtConverter().IdToObjectHDTResource(tripleID.getObject());
                    if (logger.isTraceEnabled()) {
                        logger.trace("From HDT   {} {} {} ", subject.stringValue(), predicate.stringValue(), object.stringValue());
                    }
                    next = hybridTripleSource.getValueFactory().createStatement(subject, predicate, object);
                    return true;
                }
            }
        }
        // iterate over the result of rdf4j
        if (this.repositoryResult != null && this.repositoryResult.hasNext()) {
            Statement stm = repositoryResult.next();
            Resource newSubj = hybridStore.getHdtConverter().rdf4jToHdtIDsubject(stm.getSubject());
            IRI newPred = hybridStore.getHdtConverter().rdf4jToHdtIDpredicate(stm.getPredicate());
            Value newObject = hybridStore.getHdtConverter().rdf4jToHdtIDobject(stm.getObject());
            next =  hybridTripleSource.getValueFactory().createStatement(newSubj, newPred, newObject, stm.getContext());
                if (logger.isTraceEnabled()) {
                logger.trace("From RDF4j {} {} {}", next.getSubject().stringValue(), next.getPredicate().stringValue(), next.getObject().stringValue());
            }
            return true;
        }
        return false;
    }

    @Override
    public Statement next() {
        if (!hasNext()) {
            return null;
        }
        Statement stm = hybridTripleSource.getValueFactory().createStatement(next.getSubject(), next.getPredicate(), next.getObject(), next.getContext());
        next = null;
        return stm;
    }
}

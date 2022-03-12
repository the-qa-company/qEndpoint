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

    private HybridStore hybridStore;
    private HybridTripleSource hybridTripleSource;
    private IteratorTripleID iterator;
    private CloseableIteration<? extends Statement, SailException> repositoryResult;

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
        // iterate over the result of hdt
        if (iterator != null) {
            while (iterator.hasNext()) {
                TripleID tripleID = iterator.next();
                if (tripleID.getIndex() != -1 && !hybridStore.getDeleteBitMap().access(tripleID.getIndex() - 1)) {
                    Resource subject = hybridStore.getHdtConverter().IdToSubjectHDTResource(tripleID.getSubject());
                    IRI predicate = hybridStore.getHdtConverter().IdToPredicateHDTResource(tripleID.getPredicate());
                    Value object = hybridStore.getHdtConverter().IdToObjectHDTResource(tripleID.getObject());
                    logger.trace("HDT {} {} {} ",subject.stringValue(), predicate.stringValue(),object.stringValue());
                    next = hybridTripleSource.getValueFactory().createStatement(subject, predicate, object);
                    return true;
                }
            }
        }
        // iterate over the result of rdf4j
        if (this.repositoryResult != null && this.repositoryResult.hasNext()) {
            Statement stm = repositoryResult.next();
            Resource newSubj = hybridStore.getHdtConverter().rdf4jToHdtIDsubject(stm.getSubject());
            Value newPred = hybridStore.getHdtConverter().rdf4jToHdtIDpredicate(stm.getPredicate());
            Value newObject = hybridStore.getHdtConverter().rdf4jToHdtIDobject(stm.getObject());
            next =  hybridTripleSource.getValueFactory().createStatement(newSubj, (IRI) newPred, newObject, stm.getContext());
            //logger.trace("From RDF4j {} {} {}", next.getSubject().stringValue(), next.getPredicate().stringValue(), next.getObject().stringValue());
            return true;
        }
        return false;
    }

    @Override
    public Statement next() {
        Statement stm = hybridTripleSource.getValueFactory().createStatement(next.getSubject(), next.getPredicate(), next.getObject(), next.getContext());
        return stm;
    }
}

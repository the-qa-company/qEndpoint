package com.the_qa_company.q_endpoint.utils;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.hybridstore.HybridTripleSource;
import com.the_qa_company.q_endpoint.model.HDTStatement;
import com.the_qa_company.q_endpoint.model.SimpleIRIHDT;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.SailException;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class CombinedTripleIterator implements Iterator<Statement> {
    private static final Logger logger = LoggerFactory.getLogger(CombinedTripleIterator.class);

    private HybridStore hybridStore;
    private HybridTripleSource hybridTripleSource;
    private IteratorTripleID iterator;
    private CloseableIteration<? extends Statement, SailException> repositoryResult;

    public CombinedTripleIterator(HybridStore hybridStore, HybridTripleSource hybridTripleSource, IteratorTripleID iter,
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
                logger.debug("HDT {} {} {} ",tripleID.getSubject(), tripleID.getPredicate(),tripleID.getObject());

//                SimpleIRIHDT subject;
//                if (tripleID.getSubject() <= hybridStore.getHdt().getDictionary().getNshared()) {
//                    subject = new SimpleIRIHDT(hybridStore.getHdt(),SimpleIRIHDT.SHARED_POS,tripleID.getSubject());
//                } else {
//                    subject = new SimpleIRIHDT(hybridStore.getHdt(),SimpleIRIHDT.SUBJECT_POS,tripleID.getSubject());
//                }
//                SimpleIRIHDT predicate =  new SimpleIRIHDT(hybridStore.getHdt(),SimpleIRIHDT.PREDICATE_POS,tripleID.getPredicate());
//                SimpleIRIHDT object;
//                if (tripleID.getObject() <= hybridStore.getHdt().getDictionary().getNshared()) {
//                    object = new SimpleIRIHDT(hybridStore.getHdt(),SimpleIRIHDT.SHARED_POS,tripleID.getObject());
//                } else {
//                    object = new SimpleIRIHDT(hybridStore.getHdt(),SimpleIRIHDT.OBJECT_POS,tripleID.getObject());
//                }
//                System.out.println(subject.stringValue()+" "+predicate.stringValue()+" "+object.stringValue());
//                Statement stm = hybridTripleSource.getValueFactory().createStatement(subject, predicate, object);
                  Statement stm = new HDTStatement(hybridStore, hybridTripleSource, tripleID);
                if (tripleID.getIndex() != -1 && !hybridStore.getDeleteBitMap().access(tripleID.getIndex() - 1)) {

                    next = stm;
                    return true;
                }
            }
        }
        // iterate over the result of rdf4j
        if (this.repositoryResult != null && this.repositoryResult.hasNext()) {
            Statement stm = repositoryResult.next();
            next = convertStatement(stm);
            logger.debug("From RDF4j {} {} {}", next.getSubject().stringValue(), next.getPredicate().stringValue(), next.getObject().stringValue());
            return true;
        }
        return false;
    }

    private Statement convertStatement(Statement stm) {

        Resource subject = stm.getSubject();
        Resource newSubj = hybridStore.getIriConverter().getIRIHdtSubj(subject);
        IRI predicate = stm.getPredicate();
        Value newPred = hybridStore.getIriConverter().getIRIHdtPred(predicate);
        Value newObject = hybridStore.getIriConverter().getIRIHdtObj(stm.getObject());
        return hybridTripleSource.getValueFactory().createStatement(newSubj, (IRI) newPred, newObject, stm.getContext());


    }

    @Override
    public Statement next() {
        Statement stm = hybridTripleSource.getValueFactory().createStatement(next.getSubject(), next.getPredicate(), next.getObject(), next.getContext());
        return stm;
    }
}

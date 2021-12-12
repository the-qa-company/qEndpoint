package com.the_qa_company.q_endpoint.utils;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.hybridstore.HybridTripleSource;
import com.the_qa_company.q_endpoint.model.HDTStatement;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.SailException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;

import java.util.Iterator;

public class TripleWithDeleteIter implements Iterator<Statement> {

    private HybridStore hybridStore;
    private HybridTripleSource hybridTripleSource;
    private IteratorTripleID iterator;
    private CloseableIteration<? extends Statement, SailException> repositoryResult;

    public TripleWithDeleteIter(HybridStore hybridStore, HybridTripleSource hybridTripleSource, IteratorTripleID iter,
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
            return true;
        }
        return false;
    }

    private Statement convertStatement(Statement stm) {

        Resource subject = stm.getSubject();
        Resource newSubj = hybridStore.getIriConverter().getIRIHdtSubj(subject);
        IRI predicate = stm.getPredicate();
        Value newPred = hybridStore.getIriConverter().getIRIHdtPred(predicate);
//        if(newPred instanceof SimpleIRIHDT && ((SimpleIRIHDT)newPred).getId() == -1){
//            System.out.println("alerttttt this should not happen: "+newPred.toString());
//        }
        Value newObject = hybridStore.getIriConverter().getIRIHdtObj(stm.getObject());
        return hybridTripleSource.getValueFactory().createStatement(newSubj, (IRI) newPred, newObject, stm.getContext());


    }

    @Override
    public Statement next() {
        Statement stm = hybridTripleSource.getValueFactory().createStatement(next.getSubject(), next.getPredicate(), next.getObject(), next.getContext());
        return stm;
    }
}

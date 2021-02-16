package org.rdfhdt.hdt.rdf4j.utility;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.*;
import org.eclipse.rdf4j.sail.SailException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.rdf4j.HybridTripleSource;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;

import java.util.Iterator;

public class TripleWithDeleteIter implements Iterator<Statement> {

    private HybridTripleSource tripleSource;
    private IteratorTripleID iterator;
    private HDT hdt;
    private CloseableIteration<? extends Statement, SailException> repositoryResult;

    private IRIConverter iriConverter;
    public TripleWithDeleteIter(HybridTripleSource tripleSource, IteratorTripleID iter){
        this.tripleSource = tripleSource;
        this.iterator = iter;
        this.hdt = tripleSource.getHdt();
    }

    public TripleWithDeleteIter(HybridTripleSource tripleSource, IteratorTripleID iter, CloseableIteration<? extends Statement, SailException> repositoryResult){
        this.tripleSource = tripleSource;
        this.iterator = iter;
        this.hdt = tripleSource.getHdt();
        this.repositoryResult = repositoryResult;
        this.iriConverter = new IRIConverter(hdt);
    }
    Statement next;

    @Override
    public boolean hasNext() {
        if(iterator != null) {
            while (iterator.hasNext()) {
                TripleID tripleID = iterator.next();
                Statement stm = new HDTStatement(hdt, tripleID, tripleSource);
                if (!tripleSource.getHybridStore().getDeleteBitMap().access(tripleID.getIndex() - 1)) {
                    next = stm;
                    return true;
                }
            }
        }
        if(this.repositoryResult != null && this.repositoryResult.hasNext()) {
            Statement stm = repositoryResult.next();
            next = convertStatement(stm);
            return true;
        }
        return false;
    }
    private Statement convertStatement(Statement stm){

        Resource subject = stm.getSubject();
        SimpleIRIHDT newSubj = iriConverter.getIRIHdtSubj(subject);
        IRI predicate = stm.getPredicate();
        SimpleIRIHDT newPred = iriConverter.getIRIHdtPred(predicate);
        Value object = stm.getObject();
        SimpleIRIHDT newObj = iriConverter.getIRIHdtObj(object);

        return this.tripleSource.getValueFactory().createStatement(newSubj,newPred,newObj);
    }

    @Override
    public Statement next() {
        return next;
    }
}

package org.rdfhdt.hdt.rdf4j.utility;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.*;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.rdfhdt.hdt.exceptions.NotFoundException;
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
    }
    Statement next;

    @Override
    public boolean hasNext() {
        while (iterator.hasNext()) {
            TripleID tripleID = iterator.next();
            Statement stm = new HDTStatement(hdt, tripleID, tripleSource);
            if(!tripleSource.getHybridStore().getDeleteBitMap().access(tripleID.getIndex() -1)){
                next = stm;
                return true;
            }
        }
        if(this.repositoryResult != null && this.repositoryResult.hasNext()) {

            next = convertStatement(repositoryResult.next());
            return true;
        }
        return false;
    }
    private Statement convertStatement(Statement stm){
        Resource subject = stm.getSubject();
        SimpleIRIHDT newSubj = getIRIHdtSubj(subject);
        IRI predicate = stm.getPredicate();
        SimpleIRIHDT newPred = getIRIHdtPred(predicate);
        Value object = stm.getObject();
        SimpleIRIHDT newObj = getIRIHdtObj(object);

        return this.tripleSource.getValueFactory().createStatement(newSubj,newPred,newObj);
    }
    private SimpleIRIHDT getIRIHdtSubj(Resource subj){
        String iriString = subj.toString();
        long id = -1;
        int position = -1;
        if(iriString.startsWith(("http://hdt.org/"))){
            iriString = iriString.replace("http://hdt.org/","");
            if(iriString.startsWith("SO")){
                id = Long.parseLong(iriString.substring(2));
                position = SimpleIRIHDT.SHARED_POS;
            }else if(iriString.startsWith("S")){
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.SUBJECT_POS;
            }
            return new SimpleIRIHDT(this.hdt,position,id);
        }else{ // string was not converted upon insert - iriString the real IRI
            return new SimpleIRIHDT(this.hdt,iriString);
        }
    }
    private SimpleIRIHDT getIRIHdtPred(IRI pred){
        String iriString = pred.toString();
        long id = -1;
        int position = -1;
        if(iriString.startsWith(("http://hdt.org/"))){
            iriString = iriString.replace("http://hdt.org/","");
            if(iriString.startsWith("P")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.PREDICATE_POS;
            }
            return new SimpleIRIHDT(this.hdt,position,id);
        }else{ // string was not converted upon insert - iriString the real IRI
            return new SimpleIRIHDT(this.hdt,iriString);
        }
    }
    private SimpleIRIHDT getIRIHdtObj(Value object){
        String iriString = object.toString();
        long id = -1;
        int position = -1;
        if(iriString.startsWith(("http://hdt.org/"))){
            iriString = iriString.replace("http://hdt.org/","");
            if(iriString.startsWith("SO")){
                id = Long.parseLong(iriString.substring(2));
                position = SimpleIRIHDT.SHARED_POS;
            }else if(iriString.startsWith("O")){
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.OBJECT_POS;
            }
            return new SimpleIRIHDT(this.hdt,position,id);
        }else{ // string was not converted upon insert - iriString the real IRI
            return new SimpleIRIHDT(this.hdt,iriString);
        }
    }
    @Override
    public Statement next() {
        return next;
    }
}

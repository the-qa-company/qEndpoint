package org.rdfhdt.hdt.rdf4j.utility;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.HDTStatement;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.eclipse.rdf4j.model.impl.SimpleLiteralHDT;
import org.eclipse.rdf4j.sail.SailConnection;
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
    public TripleWithDeleteIter(HybridTripleSource tripleSource, IteratorTripleID iter){
        this.tripleSource = tripleSource;
        this.iterator = iter;
        this.hdt = tripleSource.getHdt();
    }
    Statement next;

    @Override
    public boolean hasNext() {
        while (iterator.hasNext()) {
            TripleID tripleID = iterator.next();
//            long subj = tripleID.getSubject() -1;
//            long pred = tripleID.getPredicate() -1;
//            long obj = tripleID.getObject() -1;
//            if(!(tripleSource.getHybridStore().getSubjVec().access(subj) &&
//                    tripleSource.getHybridStore().getObjVec().access(obj ) &&
//                    tripleSource.getHybridStore().getPredVec().access(pred))) {
//                return new HDTStatement(hdt, tripleID, tripleSource);
//            }
//            long subjId = tripleID.getSubject();
//            long predId = tripleID.getPredicate();
//            long objId = tripleID.getObject();
            Statement stm = new HDTStatement(hdt, tripleID, tripleSource);

            if(!tripleSource.getHybridStore().getDeleteBitMap().access(tripleID.getIndex() -1)){
                next = stm;
                return true;
            }
//            Resource subj = tripleSource.getHybridStore().getValueFactory().createIRI("http://hdt-"+subjId);
//            IRI pred = tripleSource.getHybridStore().getValueFactory().createIRI("http://hdt-"+predId);
//            IRI obj = tripleSource.getHybridStore().getValueFactory().createIRI("http://hdt-"+objId);
//            // check if the statement was deleted already... in deleteStore
//
//            if(!tripleSource.getHybridStore().getDeleteStoreConnection().hasStatement(subj,pred,obj,false,(Resource)null)){
//                next = stm;
//                return true;
//            }
        }
        if(tripleSource.getRepositoryResult().hasNext()) {
            next = tripleSource.getRepositoryResult().next();
            return true;
        }
        return false;
    }

    @Override
    public Statement next() {
        return next;
    }
}

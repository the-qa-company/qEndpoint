package org.rdfhdt.hdt.rdf4j.utility;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.eclipse.rdf4j.model.impl.SimpleLiteralHDT;
import org.eclipse.rdf4j.sail.SailConnection;
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
            Statement stm = new Statement() {
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
                    if (tripleID.getObject() >= tripleSource.getStartLiteral() && tripleID.getObject() <= tripleSource.getEndLiteral()) {
                        // System.out.println("Literal "+tripleID.getObject()+" --
                        // "+hdt.getDictionary().idToString(tripleID.getObject(),TripleComponentRole.OBJECT));
                        return new SimpleLiteralHDT(hdt, tripleID.getObject(), tripleSource.getValueFactory());
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
            Resource subj = tripleSource.getValueFactory().createIRI(stm.getSubject().toString());
            IRI pred = tripleSource.getValueFactory().createIRI(stm.getPredicate().toString());
            IRI obj = tripleSource.getValueFactory().createIRI(stm.getObject().toString());
            // check if the statement was deleted already... in deleteStore
            if(!tripleSource.getHybridStore().getDeleteStoreConnection().hasStatement(subj,pred,obj,false,(Resource)null)){
                next = stm;
                return true;
            }
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

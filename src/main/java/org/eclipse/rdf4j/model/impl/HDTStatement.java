package org.eclipse.rdf4j.model.impl;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.rdf4j.HybridTripleSource;
import org.rdfhdt.hdt.triples.TripleID;

public class HDTStatement implements Statement {

    private TripleID tripleID;
    private HDT hdt;
    private HybridTripleSource tripleSource;
    public HDTStatement(HDT hdt,TripleID tripleID,HybridTripleSource tripleSource){
        this.tripleID = tripleID;
        this.hdt = hdt;
        this.tripleSource = tripleSource;
    }
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

    @Override
    public String toString() {
        return "("+getSubject().toString()+", "+getPredicate().toString()+", "+getObject()+")";
    }
}

package org.eclipse.rdf4j.model.impl;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.rdf4j.HybridTripleSource;
import org.rdfhdt.hdt.triples.TripleID;

import java.util.Objects;

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
        if(tripleID.getSubject() >= tripleSource.getHybridStore().getHdtProps().getStartBlankShared()
                && tripleID.getSubject() <= tripleSource.getHybridStore().getHdtProps().getEndBlankShared()){
            return this.tripleSource.getValueFactory().createBNode(
                    this.hdt.getDictionary().idToString(tripleID.getSubject(), TripleComponentRole.SUBJECT).toString());
        }else {
            if (tripleID.getSubject() <= hdt.getDictionary().getNshared()) {
                return new SimpleIRIHDT(hdt, SimpleIRIHDT.SHARED_POS, tripleID.getSubject());
            } else {
                return new SimpleIRIHDT(hdt, SimpleIRIHDT.SUBJECT_POS, tripleID.getSubject());
            }
        }
    }

    @Override
    public IRI getPredicate() {
        return new SimpleIRIHDT(hdt, SimpleIRIHDT.PREDICATE_POS ,tripleID.getPredicate());
    }

    @Override
    public Value getObject() {
        if (tripleID.getObject() >= tripleSource.getStartLiteral() && tripleID.getObject() <= tripleSource.getEndLiteral()) {
            return new SimpleLiteralHDT(hdt, tripleID.getObject(), tripleSource.getValueFactory());
        }else if( (tripleID.getObject() >= tripleSource.getHybridStore().getHdtProps().getStartBlankObjects()
                && tripleID.getObject() <= tripleSource.getHybridStore().getHdtProps().getEndBlankObjects())
                || (tripleID.getObject() >= tripleSource.getHybridStore().getHdtProps().getStartBlankShared()
                && tripleID.getObject() <= tripleSource.getHybridStore().getHdtProps().getEndBlankShared())
        ){
            return this.tripleSource.getValueFactory().createBNode(
                    this.hdt.getDictionary().idToString(tripleID.getObject(), TripleComponentRole.OBJECT).toString());
        } else {
            if (tripleID.getObject() <= hdt.getDictionary().getNshared()) {
                return new SimpleIRIHDT(hdt, SimpleIRIHDT.SHARED_POS,tripleID.getObject());
            } else {
                return new SimpleIRIHDT(hdt, SimpleIRIHDT.OBJECT_POS,tripleID.getObject());
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HDTStatement that = (HDTStatement) o;
        return this.tripleID.getSubject() == that.tripleID.getSubject()
                && this.tripleID.getPredicate() == that.tripleID.getObject()
                && this.tripleID.getObject() == that.tripleID.getObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(tripleID, hdt, tripleSource);
    }
}

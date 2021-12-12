package com.the_qa_company.q_endpoint.model;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.hybridstore.HybridTripleSource;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.TripleID;

import java.util.Objects;

public class HDTStatement implements Statement {

    private TripleID tripleID;
    private HybridTripleSource hybridTripleSource;
    private HybridStore hybridStore;

    public HDTStatement(HybridStore hybridStore, HybridTripleSource hybridTripleSource, TripleID tripleID) {
        this.tripleID = tripleID;
        this.hybridStore = hybridStore;
        this.hybridTripleSource = hybridTripleSource;
    }

    @Override
    public Resource getSubject() {
        if (tripleID.getSubject() >= hybridStore.getHdtProps().getStartBlankShared()
                && tripleID.getSubject() <= hybridStore.getHdtProps().getEndBlankShared()) {
            return this.hybridTripleSource.getValueFactory().createBNode(
                    hybridStore.getHdt().getDictionary().idToString(tripleID.getSubject(), TripleComponentRole.SUBJECT).toString());
        } else {
            if (tripleID.getSubject() <= hybridStore.getHdt().getDictionary().getNshared()) {
                return new SimpleIRIHDT(hybridStore.getHdt(), SimpleIRIHDT.SHARED_POS, tripleID.getSubject());
            } else {
                return new SimpleIRIHDT(hybridStore.getHdt(), SimpleIRIHDT.SUBJECT_POS, tripleID.getSubject());
            }
        }
    }

    @Override
    public IRI getPredicate() {
        return new SimpleIRIHDT(hybridStore.getHdt(), SimpleIRIHDT.PREDICATE_POS, tripleID.getPredicate());
    }

    @Override
    public Value getObject() {
        if (tripleID.getObject() >= hybridStore.getHdtProps().getStartLiteral() && tripleID.getObject() <= hybridStore.getHdtProps().getEndLiteral()) {
            return new SimpleLiteralHDT(hybridStore.getHdt(), tripleID.getObject(), hybridTripleSource.getValueFactory());
        } else if ((tripleID.getObject() >= hybridStore.getHdtProps().getStartBlankObjects()
                && tripleID.getObject() <= hybridStore.getHdtProps().getEndBlankObjects())
                || (tripleID.getObject() >= hybridStore.getHdtProps().getStartBlankShared()
                && tripleID.getObject() <= hybridStore.getHdtProps().getEndBlankShared())
        ) {
            return hybridTripleSource.getValueFactory().createBNode(
                    hybridStore.getHdt().getDictionary().idToString(tripleID.getObject(), TripleComponentRole.OBJECT).toString());
        } else {
            if (tripleID.getObject() <= hybridStore.getHdt().getDictionary().getNshared()) {
                return new SimpleIRIHDT(hybridStore.getHdt(), SimpleIRIHDT.SHARED_POS, tripleID.getObject());
            } else {
                return new SimpleIRIHDT(hybridStore.getHdt(), SimpleIRIHDT.OBJECT_POS, tripleID.getObject());
            }
        }
    }

    @Override
    public Resource getContext() {
        return null;
    }

    @Override
    public String toString() {
        return "(" + getSubject().toString() + ", " + getPredicate().toString() + ", " + getObject() + ")";
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
        return Objects.hash(tripleID, hybridStore.getHdt(), hybridTripleSource);
    }
}

package eu.qanswer.model;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.URIUtil;

import java.util.Objects;

public class SimpleIRI2 implements IRI {
    private static final long serialVersionUID = -7330406348751485330L;
    private String iriString;
    private int localNameIdx;

    protected SimpleIRI2() {
    }

    protected SimpleIRI2(String iriString) {
        this.setIRIString(iriString);
    }

    protected void setIRIString(String iriString) {
        Objects.requireNonNull(iriString, "iriString must not be null");
        if (iriString.indexOf(58) < 0) {
            throw new IllegalArgumentException("Not a valid (absolute) IRI: " + iriString);
        } else {
            this.iriString = iriString;
            this.localNameIdx = -1;
        }
    }

    public String toString() {
        return this.iriString;
    }

    public String stringValue() {
        return this.iriString;
    }

    public String getNamespace() {
        if (this.localNameIdx < 0) {
            this.localNameIdx = URIUtil.getLocalNameIndex(this.iriString);
        }

        return this.iriString.substring(0, this.localNameIdx);
    }

    public String getLocalName() {
        if (this.localNameIdx < 0) {
            this.localNameIdx = URIUtil.getLocalNameIndex(this.iriString);
        }

        return this.iriString.substring(this.localNameIdx);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else {
            return o instanceof IRI ? this.toString().equals(o.toString()) : false;
        }
    }

    public int hashCode() {
        return this.iriString.hashCode();
    }
}

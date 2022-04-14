package com.the_qa_company.qendpoint.model;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;

import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;

public class SimpleLiteral2 implements Literal {
    private static final long serialVersionUID = -1649571784782592271L;
    private String label;
    private String language;
    private IRI datatype;
    private CoreDatatype coreDatatype;

    protected SimpleLiteral2(String label) {
        this.setLabel(label);
        this.setDatatype(XSD.STRING);
    }

    protected SimpleLiteral2(String label, String language) {
        this.setLabel(label);
        this.setLanguage(language);
    }

    protected SimpleLiteral2(String label, IRI datatype) {
        this.setLabel(label);
        if (RDF.LANGSTRING.equals(datatype)) {
            throw new IllegalArgumentException("datatype rdf:langString requires a language tag");
        } else {
            if (datatype == null) {
                datatype = XSD.STRING;
            }

            this.setDatatype(datatype);
        }
    }

    protected void setLabel(String label) {
        Objects.requireNonNull(label, "Literal label cannot be null");
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

    protected void setLanguage(String language) {
        Objects.requireNonNull(language);
        if (language.isEmpty()) {
            throw new IllegalArgumentException("Language tag cannot be empty");
        } else {
            this.language = language;
            this.setDatatype(RDF.LANGSTRING);
        }
    }

    public Optional<String> getLanguage() {
        return Optional.ofNullable(this.language);
    }

    protected void setDatatype(IRI datatype) {
        this.datatype = datatype;
    }

    public IRI getDatatype() {
        return this.datatype;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o instanceof Literal) {
            Literal other = (Literal) o;
            if (!this.label.equals(other.getLabel())) {
                return false;
            } else if (!this.datatype.equals(other.getDatatype())) {
                return false;
            } else if (this.getLanguage().isPresent() && other.getLanguage().isPresent()) {
                return (this.getLanguage().get())
                        .equalsIgnoreCase(other.getLanguage().get());
            } else {
                return this.getLanguage().isEmpty() && other.getLanguage().isEmpty();
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        return this.label.hashCode();
    }

    public String toString() {
        StringBuilder sb;
        if (Literals.isLanguageLiteral(this)) {
            sb = new StringBuilder(this.label.length() + this.language.length() + 3);
            sb.append('"').append(this.label).append('"');
            sb.append('@').append(this.language);
            return sb.toString();
        } else if (!XSD.STRING.equals(this.datatype) && this.datatype != null) {
            sb = new StringBuilder(this.label.length() + this.datatype.stringValue().length() + 6);
            sb.append('"').append(this.label).append('"');
            sb.append("^^<").append(this.datatype.toString()).append(">");
            return sb.toString();
        } else {
            sb = new StringBuilder(this.label.length() + 2);
            sb.append('"').append(this.label).append('"');
            return sb.toString();
        }
    }

    public String stringValue() {
        return this.label;
    }

    public boolean booleanValue() {
        return XMLDatatypeUtil.parseBoolean(this.getLabel());
    }

    public byte byteValue() {
        return XMLDatatypeUtil.parseByte(this.getLabel());
    }

    public short shortValue() {
        return XMLDatatypeUtil.parseShort(this.getLabel());
    }

    public int intValue() {
        return XMLDatatypeUtil.parseInt(this.getLabel());
    }

    public long longValue() {
        return XMLDatatypeUtil.parseLong(this.getLabel());
    }

    public float floatValue() {
        return XMLDatatypeUtil.parseFloat(this.getLabel());
    }

    public double doubleValue() {
        return XMLDatatypeUtil.parseDouble(this.getLabel());
    }

    public BigInteger integerValue() {
        return XMLDatatypeUtil.parseInteger(this.getLabel());
    }

    public BigDecimal decimalValue() {
        return XMLDatatypeUtil.parseDecimal(this.getLabel());
    }

    public XMLGregorianCalendar calendarValue() {
        return XMLDatatypeUtil.parseCalendar(this.getLabel());
    }

    @Override
    public CoreDatatype getCoreDatatype() {
        if (coreDatatype == null) {
            coreDatatype = CoreDatatype.from(getDatatype());
        }
        return coreDatatype;
    }
}

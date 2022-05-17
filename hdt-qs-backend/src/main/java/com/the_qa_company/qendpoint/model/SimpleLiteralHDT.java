/**
 * ***************************************************************************** Copyright (c) 2015
 * Eclipse RDF4J contributors, Aduna, and others. All rights reserved. This program and the
 * accompanying materials are made available under the terms of the Eclipse Distribution License
 * v1.0 which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 * *****************************************************************************
 */
package com.the_qa_company.qendpoint.model;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;

import static org.eclipse.rdf4j.rio.helpers.NTriplesUtil.parseURI;

/**
 * A implementation of the {@link Literal} interface for HDT.
 *
 * @author Dennis Diefenbach
 */
public class SimpleLiteralHDT implements Literal {

    /*-----------*
     * Constants *
     *-----------*/

    private static final long serialVersionUID = -1649571784782592271L;

    /*-----------*
     * Variables *
     *-----------*/

    /**
     * The literal's hdt ID.
     */
    private HDT hdt;

    private final ValueFactory valueFactory;

    /**
     * The literal's hdt ID.
     */
    private long hdtID;

    /**
     * The literal's label.
     */
    private String label;

    /**
     * The literal's language tag.
     */
    private Optional<String> language;

    /**
     * The literal's datatype.
     */
    private IRI datatype;

    private CoreDatatype coreDatatype;

    private final boolean optimizeDatatype;

    /*--------------*
     * Constructors *
     *--------------*/

    /**
     * Creates a new plain literal with the supplied label.
     *
     * @param hdt
     *            The hdt file in which the literal is contained, must not be <tt>null</tt>.
     */
    public SimpleLiteralHDT(HDT hdt, long id, ValueFactory factory, boolean optimizeDatatype) {
        setHdt(hdt);
        setHdtID(id);
        valueFactory = factory;
        this.optimizeDatatype = optimizeDatatype;
    }

    /*---------*
     * Methods *
     *---------*/

    protected void setHdt(HDT hdt) {
        Objects.requireNonNull(hdt, "Literal label cannot be null");
        this.hdt = hdt;
    }

    protected void setHdtID(long id) {
        this.hdtID = id;
    }

    protected void parseDatatype() {
        if (!optimizeDatatype) {
            parseLiteral();
            return;
        }
        if (datatype == null) {
            String datatype = hdt.getDictionary().dataTypeOfId(hdtID);
            if (datatype.isEmpty()) {
                parseLiteral(); // we need to check if it is a LANGSTRING or not
            } else {
                this.datatype = NTriplesUtil.parseURI(datatype, valueFactory);
            }
        }
    }

    private static int lastIndexOfQuote(CharSequence seq) {
        for (int i = seq.length() - 1; i >= 0; --i) {
            if (seq.charAt(i) == '"') {
                return i;
            }
        }
        return -1;
    }

    static int indexOf(CharSequence seq, CharSequence s, int start) {
        int n = seq.length() - s.length() + 1;
        loop: for (int i = start; i < n; i++) {
            for (int j = 0; j < s.length(); j++) {
                if (seq.charAt(i + j) != s.charAt(j)) {
                    continue loop;
                }
            }
            return i;
        }
        return -1;
    }

    static int indexOf(CharSequence seq, char c, int start) {
        for (int i = start; i < seq.length(); i++) {
            if (seq.charAt(i) == c) {
                return i;
            }
        }
        return -1;
    }

    protected void parseLiteral() {
        if (label == null) {
            try {
                CharSequence literal = hdt.getDictionary().idToString(hdtID, TripleComponentRole.OBJECT);
                if (literal.length() > 0 && literal.charAt(0) == '"') {
                    int endLabelIdx = lastIndexOfQuote(literal);
                    if (endLabelIdx != -1) {
                        int startLangIdx = indexOf(literal, '@', endLabelIdx + 1);
                        int startDtIdx = indexOf(literal, "^^", endLabelIdx + 1);
                        if (startLangIdx != -1 && startDtIdx != -1) {
                            throw new IllegalArgumentException("Literals can not have both a language and a datatype");
                        }

                        label = literal.subSequence(1, endLabelIdx).toString();
                        // label = unescapeString(label);
                        if (startLangIdx != -1) {
                            datatype = CoreDatatype.RDF.LANGSTRING.getIri();
                            language = Optional.of(literal.subSequence(startLangIdx + 1, literal.length()).toString());
                        } else if (startDtIdx != -1) {
                            if (datatype == null) {
                                datatype = parseURI(literal.subSequence(startDtIdx + 2, literal.length()).toString(),
                                        valueFactory);
                            }
                            language = Optional.empty();
                        } else {
                            language = Optional.empty();
                            datatype = XSD.STRING;
                        }
                    }
                }
            } catch (IllegalArgumentException e) {
                // @todo: this should be fixed, it is for example happening for Select ?o where {
                // <http://www.wikidata.org/entity/Q29709019> ?p ?o} over wikidata
                label = "";
                language = Optional.empty();
                datatype = XSD.STRING;
            }
        }
    }

    @Override
    public String getLabel() {
        parseLiteral();
        return label;
    }

    @Override
    public Optional<String> getLanguage() {
        parseLiteral();
        return language;
    }

    @Override
    public IRI getDatatype() {
        parseDatatype();
        return datatype;
    }

    // Overrides Object.equals(Object), implements Literal.equals(Object)
    @Override
    public boolean equals(Object o) {
        // TODO: This can be probably done more efficielnty
        if (this == o) {
            return true;
        }

        if (o instanceof SimpleLiteralHDT) {
            return ((SimpleLiteralHDT) o).getHdtID() == getHdtID();
        } else if (o instanceof Literal) {
            Literal other = (Literal) o;

            // Compare datatypes
            if (!getDatatype().equals(other.getDatatype())) {
                return false;
            }

            // Compare labels
            if (!getLabel().equals(other.getLabel())) {
                return false;
            }

            return getLanguage().equals(other.getLanguage());
        }

        return false;
    }

    // overrides Object.hashCode(), implements Literal.hashCode()
    @Override
    public int hashCode() {
        return getLabel().hashCode();
    }

    /**
     * Returns the label of the literal with its language or datatype. Note that this method does not escape the quoted
     * label.
     *
     * @see org.eclipse.rdf4j.rio.helpers.NTriplesUtil#toNTriplesString(Literal)
     */
    @Override
    public String toString() {
        getLabel();
        if (Literals.isLanguageLiteral(this)) {
            return '"' + label + '"' + '@' + language;
        } else if (XSD.STRING.equals(datatype) || datatype == null) {
            return '"' + label + '"';
        } else {
            return '"' + label + '"' + "^^<" + datatype + ">";
        }
    }

    public long getHdtID() {
        return hdtID;
    }

    @Override
    public String stringValue() {
        return getLabel();
    }

    @Override
    public boolean booleanValue() {
        return XMLDatatypeUtil.parseBoolean(getLabel());
    }

    @Override
    public byte byteValue() {
        return XMLDatatypeUtil.parseByte(getLabel());
    }

    @Override
    public short shortValue() {
        return XMLDatatypeUtil.parseShort(getLabel());
    }

    @Override
    public int intValue() {
        return XMLDatatypeUtil.parseInt(getLabel());
    }

    @Override
    public long longValue() {
        return XMLDatatypeUtil.parseLong(getLabel());
    }

    @Override
    public float floatValue() {
        return XMLDatatypeUtil.parseFloat(getLabel());
    }

    @Override
    public double doubleValue() {
        return XMLDatatypeUtil.parseDouble(getLabel());
    }

    @Override
    public BigInteger integerValue() {
        return XMLDatatypeUtil.parseInteger(getLabel());
    }

    @Override
    public BigDecimal decimalValue() {
        return XMLDatatypeUtil.parseDecimal(getLabel());
    }

    @Override
    public XMLGregorianCalendar calendarValue() {
        return XMLDatatypeUtil.parseCalendar(getLabel());
    }

    @Override
    public CoreDatatype getCoreDatatype() {
        if (coreDatatype == null) {
            coreDatatype = CoreDatatype.from(getDatatype());
        }
        return coreDatatype;
    }
}

/**
 * ***************************************************************************** Copyright (c) 2015
 * Eclipse RDF4J contributors, Aduna, and others. All rights reserved. This program and the
 * accompanying materials are made available under the terms of the Eclipse Distribution License
 * v1.0 which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 * *****************************************************************************
 */
package eu.qanswer.model;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;

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

  /** The literal's hdt ID. */
  private HDT hdt;

  private ValueFactory valueFactory;

  /** The literal's hdt ID. */
  private long hdtID;

  /** The literal's label. */
  private String label;

  /** The literal's language tag. */
  private String language;

  /** The literal's datatype. */
  private IRI datatype;

  /*--------------*
   * Constructors *
   *--------------*/

  protected SimpleLiteralHDT() {}

  /**
   * Creates a new plain literal with the supplied label.
   *
   * @param hdt The hdt file in which the literal is contained, must not be <tt>null</tt>.
   */
  public SimpleLiteralHDT(HDT hdt, long id, ValueFactory factory) {
    setHdt(hdt);
    setHdtID(id);
    valueFactory = factory;
  }

  /*---------*
   * Methods *
   *---------*/

  protected void setHdt(HDT hdt) {
    Objects.requireNonNull(hdt, "Literal label cannot be null");
    this.hdt = hdt;
  }

  protected void setHdtID(long id) {
    Objects.requireNonNull(id, "Literal label cannot be null");
    this.hdtID = id;
  }

  protected void parseLiteral() {
    if (label == null) {
//       System.out.println("Parse lietral "+hdtID + " -- " + hdt.getDictionary().idToString(hdtID, TripleComponentRole.OBJECT).toString());
      try {
        String literal = hdt.getDictionary().idToString(hdtID, TripleComponentRole.OBJECT).toString();
        Literal l = LiteralParser.parseLiteral(literal, valueFactory);
        label = l.getLabel();
//        System.out.println("Parsed literal "+label);
//        System.out.println("Label from HDT:"+literal);
        if (l.getLanguage().isPresent()) {
          language = l.getLanguage().get();
        }
        datatype = l.getDatatype();
      } catch (IllegalArgumentException e) {
        // @todo: this should be fixed, it is for example happening for Select ?o where { <http://www.wikidata.org/entity/Q29709019> ?p ?o} over wikidata
        label = "";
        datatype = XMLSchema.STRING;
      }
    }
  }

  public String getLabel() {
    parseLiteral();
    return label;
  }

  public Optional<String> getLanguage() {
    parseLiteral();
    // System.out.println("Language "+ language);
    return Optional.ofNullable(language);
  }

  public IRI getDatatype() {
    parseLiteral();
    return datatype;
  }

  // Overrides Object.equals(Object), implements Literal.equals(Object)
  @Override
  public boolean equals(Object o) {
    //TODO: This can be probably done more efficielnty
    parseLiteral();
    if (this == o) {
      return true;
    }

    if (o instanceof Literal) {
      Literal other = (Literal) o;

      // Compare labels
      if (!label.equals(other.getLabel())) {
        return false;
      }

      // Compare datatypes
      if (!datatype.equals(other.getDatatype())) {
        return false;
      }

      if (getLanguage().isPresent() && other.getLanguage().isPresent()) {
        return getLanguage().get().equalsIgnoreCase(other.getLanguage().get());
      }
      // If only one has a language, then return false
      else if (getLanguage().isPresent() || other.getLanguage().isPresent()) {
        return false;
      }

      return true;
    }

    return false;
  }

  // overrides Object.hashCode(), implements Literal.hashCode()
  @Override
  public int hashCode() {
    return getLabel().hashCode();
  }

  /**
   * Returns the label of the literal with its language or datatype. Note that this method does not
   * escape the quoted label.
   *
   * @see NTriplesUtil#toNTriplesString(Literal)
   */
  @Override
  public String toString() {
    getLabel();
    if (Literals.isLanguageLiteral(this)) {
      StringBuilder sb = new StringBuilder(label.length() + language.length() + 3);
      sb.append('"').append(label).append('"');
      sb.append('@').append(language);
      return sb.toString();
    } else if (XMLSchema.STRING.equals(datatype) || datatype == null) {
      StringBuilder sb = new StringBuilder(label.length() + 2);
      sb.append('"').append(label).append('"');
      return sb.toString();
    } else {
      StringBuilder sb = new StringBuilder(label.length() + datatype.stringValue().length() + 6);
      sb.append('"').append(label).append('"');
      sb.append("^^<").append(datatype.toString()).append(">");
      return sb.toString();
    }
  }

  public long getHdtID() {
    return hdtID;
  }

  public String stringValue() {
    return getLabel();
  }

  public boolean booleanValue() {
    return XMLDatatypeUtil.parseBoolean(getLabel());
  }

  public byte byteValue() {
    return XMLDatatypeUtil.parseByte(getLabel());
  }

  public short shortValue() {
    return XMLDatatypeUtil.parseShort(getLabel());
  }

  public int intValue() {
    // System.out.println("PARSE "+XMLDatatypeUtil.parseInt(getLabel()));
    return XMLDatatypeUtil.parseInt(getLabel());
  }

  public long longValue() {
    return XMLDatatypeUtil.parseLong(getLabel());
  }

  public float floatValue() {
    return XMLDatatypeUtil.parseFloat(getLabel());
  }

  public double doubleValue() {
    return XMLDatatypeUtil.parseDouble(getLabel());
  }

  public BigInteger integerValue() {
    return XMLDatatypeUtil.parseInteger(getLabel());
  }

  public BigDecimal decimalValue() {
    return XMLDatatypeUtil.parseDecimal(getLabel());
  }

  public XMLGregorianCalendar calendarValue() {
    return XMLDatatypeUtil.parseCalendar(getLabel());
  }
}

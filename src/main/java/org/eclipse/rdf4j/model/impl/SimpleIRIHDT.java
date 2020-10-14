package org.eclipse.rdf4j.model.impl;

/**
 * ***************************************************************************** Copyright (c) 2015
 * Eclipse RDF4J contributors, Aduna, and others. All rights reserved. This program and the
 * accompanying materials are made available under the terms of the Eclipse Distribution License
 * v1.0 which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 * *****************************************************************************
 */
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

import java.util.Objects;

/** The default implementation of the {@link IRI} interface. */
public class SimpleIRIHDT implements IRI {

  /*-----------*
   * Constants *
   *-----------*/

  private static final long serialVersionUID = -7330406348751485330L;
  private HDT hdt;

  /*-----------*
   * Variables *
   *-----------*/

  /** The IRI string. */
  private String hdtId;

  /** The IRI string. */
  private String iriString;

  /**
   * An index indicating the first character of the local name in the IRI string, -1 if not yet set.
   */
  private int localNameIdx;

  /*--------------*
   * Constructors *
   *--------------*/

  /** Creates a new, un-initialized IRI. */
  protected SimpleIRIHDT(HDT hdt) {
    this.hdt = hdt;
  }

  /**
   * Creates a new IRI from the supplied string.
   *
   * <p>Note that creating SimpleIRI objects directly via this constructor is not the recommended
   * approach. Instead, use a {@link org.eclipse.rdf4j.model.ValueFactory ValueFactory} (obtained
   * from your repository or by using {@link SimpleValueFactory#getInstance()}) to create new IRI
   * objects.
   *
   * @param hdtId A String representing a valid, absolute IRI. May not be <code>null</code>.
   * @throws IllegalArgumentException If the supplied IRI is not a valid (absolute) IRI.
   * @see {@link SimpleValueFactory#createIRI(String)}
   */
  public SimpleIRIHDT(HDT hdt, String hdtId) {
    // System.out.println("Creating "+iriString);
    this.hdt = hdt;
    setHdtId(hdtId);
  }

  /*---------*
   * Methods *
   *---------*/

  protected void setHdtId(String hdtId) {
    Objects.requireNonNull(hdtId, "iriString must not be null");
    this.hdtId = hdtId;
  }

  public String getHdtId() {
    return hdtId;
  }

  // Implements IRI.toString()
  @Override
  public String toString() {
    iriString = stringValue();
    return iriString;
  }

  public String stringValue() {
    if (hdtId.startsWith("hdt:")) {
      String identifier = hdtId.replace("hdt:", "");

      if (identifier.startsWith("SO")) {
        return hdt.getDictionary()
            .idToString(
                Long.valueOf(identifier.substring(2, identifier.length())),
                TripleComponentRole.SUBJECT)
            .toString();
      } else if (identifier.startsWith("S")) {
        return hdt.getDictionary()
            .idToString(
                Long.valueOf(identifier.substring(1, identifier.length())),
                TripleComponentRole.SUBJECT)
            .toString();
      } else if (identifier.startsWith("O")) {
        return hdt.getDictionary()
            .idToString(
                Long.valueOf(identifier.substring(1, identifier.length())),
                TripleComponentRole.OBJECT)
            .toString();
      } else if (identifier.startsWith("P")) {
        return hdt.getDictionary()
            .idToString(
                Long.valueOf(identifier.substring(1, identifier.length())),
                TripleComponentRole.PREDICATE)
            .toString();
      } else {
        try {
          throw new Exception("The iri " + hdtId + "could not be mapped");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return hdtId;
  }

  public String getNamespace() {
    if (iriString == null) {
      iriString = stringValue();
    }
    //        System.out.println(hdtId);
    //        System.out.println(iriString);
    //        System.out.println(localNameIdx);
    if (localNameIdx < 0) {
      localNameIdx = URIUtil.getLocalNameIndex(iriString);
    }
    return iriString.substring(0, localNameIdx);
  }

  public String getLocalName() {
    if (localNameIdx < 0) {
      localNameIdx = URIUtil.getLocalNameIndex(iriString);
    }

    return iriString.substring(localNameIdx);
  }

  // Implements IRI.equals(Object)
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof SimpleIRIHDT) {
      return hdtId.equals(((SimpleIRIHDT) o).getHdtId());
    }
    if (o instanceof IRI) {
      return toString().equals(o.toString());
    }
    return false;
  }

  // Implements IRI.hashCode()
  @Override
  public int hashCode() {
    return hdtId.hashCode();
  }
}

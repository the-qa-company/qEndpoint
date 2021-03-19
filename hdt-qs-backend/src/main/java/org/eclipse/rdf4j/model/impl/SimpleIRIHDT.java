package org.eclipse.rdf4j.model.impl;

/**
 * ***************************************************************************** Copyright (c) 2015
 * Eclipse RDF4J contributors, Aduna, and others. All rights reserved. This program and the
 * accompanying materials are made available under the terms of the Eclipse Distribution License
 * v1.0 which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 * *****************************************************************************
 */
import eu.qanswer.enpoint.Sparql;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.AbstractIRI;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

import java.util.Objects;

/** The default implementation of the {@link IRI} interface. */
public class SimpleIRIHDT extends AbstractIRI implements IRI {

  /*-----------*
   * Constants *
   *-----------*/

  private static final long serialVersionUID = -7330406348751485330L;
  public static final int SUBJECT_POS = 1;
  public static final int PREDICATE_POS = 2;
  public static final int OBJECT_POS = 3;
  public static final int SHARED_POS = 4;


  /*-----------*
   * Variables *
   *-----------*/

  /** The IRI string. */
  private HDT hdt;
  private int postion;
  private long id;
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
   * @param position an integer represents if the IRI is a subject, object, predicate or shared
   * @throws IllegalArgumentException If the supplied IRI is not a valid (absolute) IRI.
   * @see {@link SimpleValueFactory#createIRI(String)}
   */
  public SimpleIRIHDT(HDT hdt,int position,long id) {
    this.hdt = hdt;
    this.postion = position;
    this.id = id;
  }
  public SimpleIRIHDT(HDT hdt,String iriString) {
    this.hdt = hdt;
    this.iriString = iriString;
    this.id = -1;
  }

  public long getId() {
    return id;
  }

  public int getPostion() {
    return postion;
  }
  /*---------*
   * Methods *
   *---------*/

  // Implements IRI.toString()
  @Override
  public String toString() {
    if(iriString == null)
      iriString = stringValue();
    // if not null means that it doesn't exist in hdt or already converted
    return iriString;
  }

  @Override
  public String stringValue() {
    if(this.iriString != null)
      return this.iriString;
    else {
//      Sparql.count++;
//      System.out.println(Sparql.count);
      if (this.postion == SHARED_POS) {
        return hdt.getDictionary()
                .idToString(
                        this.id,
                        TripleComponentRole.SUBJECT)
                .toString();
      } else if (this.postion == SUBJECT_POS) {
        return hdt.getDictionary()
                .idToString(
                        this.id,
                        TripleComponentRole.SUBJECT)
                .toString();
      } else if (this.postion == OBJECT_POS) {
        return hdt.getDictionary()
                .idToString(
                        this.id,
                        TripleComponentRole.OBJECT)
                .toString();
      } else if (this.postion == PREDICATE_POS) {
        return hdt.getDictionary()
                .idToString(
                        this.id,
                        TripleComponentRole.PREDICATE)
                .toString();
      } else {
        try {
          throw new Exception("The iri could not be mapped");
        } catch (Exception e) {
          e.printStackTrace();
        }
        return null;
      }
    }
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
    if(o == null)
      return false;
    if (this == o) {
      return true;
    }
    else if (o instanceof SimpleIRIHDT && this.id != -1 && ((SimpleIRIHDT) o).getId() != -1) {
      return this.id == (((SimpleIRIHDT) o).getId());
    }else { // could not compare IDs, we have to compare to string
      return toString().equals(o.toString());
    }
  }
//  @Override
//  public int hashCode() {
//    String prefix = "http://hdt.org/";
//    if(this.postion == SHARED_POS)
//      prefix +="SO";
//    else if(this.postion == SUBJECT_POS)
//      prefix += "S";
//    else if(this.postion == PREDICATE_POS)
//      prefix += "P";
//    else if(this.postion == OBJECT_POS)
//      prefix += "O";
//    else{
//      if(iriString != null)
//        prefix = iriString;
//      return prefix.hashCode();
//    }
//    prefix+=id;
//    return prefix.hashCode();
//  }
}

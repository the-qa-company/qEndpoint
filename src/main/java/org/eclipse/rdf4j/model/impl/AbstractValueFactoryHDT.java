/**
 * ***************************************************************************** Copyright (c) 2015
 * Eclipse RDF4J contributors, Aduna, and others. All rights reserved. This program and the
 * accompanying materials are made available under the terms of the Eclipse Distribution License
 * v1.0 which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 * *****************************************************************************
 */
package org.eclipse.rdf4j.model.impl;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.AbstractValueFactory;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

/**
 * Abstract base class for {@link ValueFactory} implementations. It implements all basic {@link
 * Value} creation methods by using the default implementations ({@link SimpleBNode}, {@link
 * SimpleIRI}, etc), and type-optimized subclasses (e.g. {@link BooleanLiteral}, {@link
 * NumericLiteral}) where possible.
 *
 * @author Arjohn Kampman
 * @author Jeen Broekstra
 */
public class AbstractValueFactoryHDT extends AbstractValueFactory {

  private HDT hdt;

  /*--------------*
   * Constructors *
   *--------------*/

  public AbstractValueFactoryHDT(HDT hdt) {
    super();
    this.hdt = hdt;
  }

  /*---------*
   * Methods *
   *---------*/

  @Override
  public IRI createIRI(String iri) {
    return stringToId(iri);
  }
  private SimpleIRIHDT stringToId(String iriString){
    long id = hdt.getDictionary().stringToId(iriString, TripleComponentRole.SUBJECT);
    int position = -1;
    if (id != -1) {
      if (id <= hdt.getDictionary().getNshared()) {
        position = SimpleIRIHDT.SHARED_POS;
      } else {
        position = SimpleIRIHDT.SUBJECT_POS;
      }
    } else {
      id = hdt.getDictionary().stringToId(iriString, TripleComponentRole.OBJECT);
      if (id != -1) {
        position = SimpleIRIHDT.OBJECT_POS;
      } else {
        id = hdt.getDictionary()
                        .stringToId(iriString, TripleComponentRole.PREDICATE);
        if (id != -1) {
          position = SimpleIRIHDT.PREDICATE_POS;
        }
      }
    }
    if(id != -1){
      return new SimpleIRIHDT(hdt,position,id);
    }else{
      return new SimpleIRIHDT(hdt,iriString);
    }
  }

  @Override
  public IRI createIRI(String namespace, String localName) {
    return createIRI(namespace + localName);
  }


  @Override
  public Literal createLiteral(String value) {
    return new SimpleLiteral2(value);
  }

  @Override
  public Literal createLiteral(String value, String language) {
    return new SimpleLiteral2(value, language);
  }

  @Override
  public Literal createLiteral(String value, IRI datatype) {
    return new SimpleLiteral2(value, datatype);
  }

}

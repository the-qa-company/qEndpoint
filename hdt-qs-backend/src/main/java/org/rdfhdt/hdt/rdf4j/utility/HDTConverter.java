package org.rdfhdt.hdt.rdf4j.utility;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

public class HDTConverter {

  private HDT hdt;

  public HDTConverter(HDT hdt) {
    this.hdt = hdt;
  }

  public long subjectId(Resource resource) {

    long subject = -1;

    if (resource == null) {
      subject = 0;
    }else if(!(resource instanceof SimpleIRIHDT)){
        subject = hdt.getDictionary().stringToId(resource.toString(), TripleComponentRole.SUBJECT);
    } else {
      long id = ((SimpleIRIHDT) resource).getId();
      long position = ((SimpleIRIHDT) resource).getPostion();
      if (id == -1) { // no id found when conversion
        return -1;
      } else {
        if (position == SimpleIRIHDT.PREDICATE_POS) {
          String translate =
                  hdt.getDictionary().idToString(id, TripleComponentRole.PREDICATE).toString();
          long translatedId =
                  hdt.getDictionary().stringToId(translate, TripleComponentRole.SUBJECT);
          if (translatedId != -1) {
            subject = translatedId;
          }
        }else if(position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.SUBJECT_POS){
          subject = id;
        }
      }
    }
    return subject;
  }

  public long predicateId(IRI iri) {
    long predicate = -1;
    if (iri == null) {
      predicate = 0;
    }else if( !(iri instanceof SimpleIRIHDT)){
      predicate = hdt.getDictionary().stringToId(iri.toString(), TripleComponentRole.PREDICATE);
    } else {
      long id = ((SimpleIRIHDT) iri).getId();
      long position = ((SimpleIRIHDT) iri).getPostion();
      if(id == -1){ // no id found when conversion
        return -1;
      }else{
        if (position == SimpleIRIHDT.PREDICATE_POS) {
          predicate = id;
        } else {
          if (position == SimpleIRIHDT.SHARED_POS) {
            String translate =
                    hdt.getDictionary()
                            .idToString(id,
                                    TripleComponentRole.SUBJECT)
                            .toString();
            long translatedId =
                    hdt.getDictionary().stringToId(translate, TripleComponentRole.PREDICATE);
            if (translatedId != -1) {
              predicate = translatedId;
            }
          } else if (position == SimpleIRIHDT.SUBJECT_POS) {
            String translate =
                    hdt.getDictionary()
                            .idToString(id,
                                    TripleComponentRole.SUBJECT)
                            .toString();
            long translatedId =
                    hdt.getDictionary().stringToId(translate, TripleComponentRole.PREDICATE);
            if (translatedId != -1) {
              predicate = translatedId;
            }
          } else if (position == SimpleIRIHDT.OBJECT_POS) {
            String translate =
                    hdt.getDictionary()
                            .idToString(id,
                                    TripleComponentRole.OBJECT)
                            .toString();
            long translatedId =
                    hdt.getDictionary().stringToId(translate, TripleComponentRole.PREDICATE);
            if (translatedId != -1) {
              predicate = translatedId;
            }
          } else {
            predicate = -1;
          }
        }
      }
    }
    return predicate;
  }

  public long objectId(Value value) {
    long object = -1;
    if (value == null) {
      object = 0;
    }else if( !(value instanceof SimpleIRIHDT)){
      String str = value.toString();

      object = hdt.getDictionary().stringToId(value.toString(), TripleComponentRole.OBJECT);
    }else {
      long id = ((SimpleIRIHDT) value).getId();
      long position = ((SimpleIRIHDT) value).getPostion();
      if(id == -1){
        return -1;
      }else {
        if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.OBJECT_POS) {
          object = id;
        } else {
          if (position == SimpleIRIHDT.PREDICATE_POS) {
            String translate =
                    hdt.getDictionary().idToString(id, TripleComponentRole.PREDICATE).toString();
            long translatedId = hdt.getDictionary().stringToId(translate, TripleComponentRole.OBJECT);
            if (translatedId != -1) {
              object = translatedId;
            }
          }
        }
      }
    }
    return object;
  }
}

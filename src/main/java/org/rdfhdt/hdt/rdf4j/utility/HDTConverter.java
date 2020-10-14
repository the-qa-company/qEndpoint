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
    } else if (!(resource instanceof SimpleIRIHDT)) {
      subject = hdt.getDictionary().stringToId(resource.toString(), TripleComponentRole.SUBJECT);

    } else {
      String hdtStr = ((SimpleIRIHDT) resource).getHdtId();
      String identifier = hdtStr.replace("hdt:", "");
      if (identifier.startsWith("SO")) {
        subject = Long.valueOf(identifier.substring(2, identifier.length()));
      } else if (identifier.startsWith("S")) {
        subject = Long.valueOf(identifier.substring(1, identifier.length()));
      } else {
        if (identifier.startsWith("P")) {
          Long id = Long.valueOf(identifier.substring(1, identifier.length()));
          String translate =
              hdt.getDictionary().idToString(id, TripleComponentRole.PREDICATE).toString();
          long translatedId =
              hdt.getDictionary().stringToId(translate, TripleComponentRole.SUBJECT);
          if (translatedId != -1) {
            subject = translatedId;
          }

        } else {
          subject = -1;
        }
      }
    }
    return subject;
  }

  public long predicateId(IRI iri) {
    long predicate = -1;
    if (iri == null) {
      predicate = 0;
    } else if (!(iri instanceof SimpleIRIHDT)) {
      predicate = hdt.getDictionary().stringToId(iri.toString(), TripleComponentRole.PREDICATE);
    } else {
      String identifier = ((SimpleIRIHDT) iri).getHdtId().replace("hdt:", "");
      if (identifier.startsWith("P")) {
        predicate = Long.valueOf(identifier.substring(1, identifier.length()));
      } else {
        if (identifier.startsWith("SO")) {
          String translate =
              hdt.getDictionary()
                  .idToString(
                      Long.valueOf(identifier.substring(2, identifier.length())),
                      TripleComponentRole.SUBJECT)
                  .toString();
          long translatedId =
              hdt.getDictionary().stringToId(translate, TripleComponentRole.PREDICATE);
          if (translatedId != -1) {
            predicate = translatedId;
          }
        } else if (identifier.startsWith("S")) {
          String translate =
              hdt.getDictionary()
                  .idToString(
                      Long.valueOf(identifier.substring(1, identifier.length())),
                      TripleComponentRole.SUBJECT)
                  .toString();
          long translatedId =
              hdt.getDictionary().stringToId(translate, TripleComponentRole.PREDICATE);
          if (translatedId != -1) {
            predicate = translatedId;
          }
        } else if (identifier.startsWith("O")) {
          String translate =
              hdt.getDictionary()
                  .idToString(
                      Long.valueOf(identifier.substring(1, identifier.length())),
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
    return predicate;
  }

  public long objectId(Value value) {
    long object = -1;
    if (value == null) {
      object = 0;
    } else if (value instanceof SimpleIRIHDT) {
      String identifier = ((SimpleIRIHDT) value).getHdtId().replace("hdt:", "");
      if (identifier.startsWith("SO")) {
        object = Long.valueOf(identifier.substring(2, identifier.length()));
      } else if (identifier.startsWith("O")) {
        object = Long.valueOf(identifier.substring(1, identifier.length()));
      } else {
        if (identifier.startsWith("P")) {
          Long id = Long.valueOf(identifier.substring(1, identifier.length()));
          String translate =
              hdt.getDictionary().idToString(id, TripleComponentRole.PREDICATE).toString();
          long translatedId = hdt.getDictionary().stringToId(translate, TripleComponentRole.OBJECT);
          if (translatedId != -1) {
            object = translatedId;
          }
        } else {
          object = -1;
        }
      }
    } else {
      object = hdt.getDictionary().stringToId(value.toString(), TripleComponentRole.OBJECT);
    }
    return object;
  }
}

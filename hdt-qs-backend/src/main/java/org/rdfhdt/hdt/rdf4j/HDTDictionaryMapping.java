package org.rdfhdt.hdt.rdf4j;

import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

public class HDTDictionaryMapping {
  HDT hdt;
  long numberSubjects;
  long numberPredicates;

  public HDTDictionaryMapping(HDT hdt) {
    this.hdt = hdt;
    this.numberSubjects = hdt.getDictionary().getNsubjects();
    this.numberPredicates = hdt.getDictionary().getNpredicates();
  }

  public long stringToId(String s) {
    long id = hdt.getDictionary().stringToId(s, TripleComponentRole.PREDICATE);
    if (id == -1) {
      id = hdt.getDictionary().stringToId(s, TripleComponentRole.SUBJECT);
      if (id == -1) {
        return numberPredicates + id;
      } else {
        id = hdt.getDictionary().stringToId(s, TripleComponentRole.OBJECT);
        if (id != -1) {
          return numberPredicates + numberSubjects + id;
        } else {
          return -1;
        }
      }
    } else {
      return id;
    }
  }

  public String idToString(long id) {
    if (id < numberPredicates) {
      return hdt.getDictionary().idToString(id, TripleComponentRole.PREDICATE).toString();
    } else {
      if (id < numberSubjects) {
        return hdt.getDictionary().idToString(id, TripleComponentRole.SUBJECT).toString();
      } else {
        return hdt.getDictionary().idToString(id, TripleComponentRole.PREDICATE).toString();
      }
    }
  }
}

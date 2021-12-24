package com.the_qa_company.q_endpoint.utils;

import com.the_qa_company.q_endpoint.model.HybridStoreValueFactory;
import com.the_qa_company.q_endpoint.model.SimpleIRIHDT;
import com.the_qa_company.q_endpoint.model.SimpleLiteralHDT;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

// class that converts an object using the HDT dictionary to an object for RDF4j and the other way around,
// the objective is to use HDT IDs as much as possible to minimize calls to the HDT dictionary when making for example joins
public class IRIConverter {
    private static final Logger logger = LoggerFactory.getLogger(IRIConverter.class);
    private HDT hdt;
    private ValueFactory valueFactory = new MemValueFactory();

    public IRIConverter(HDT hdt) {
        this.hdt = hdt;
    }

    public Resource getIRIHdtSubj(Resource subj) {
        String iriString = subj.toString();
        long id = -1;
        int position = -1;
        if (iriString.startsWith(("http://hdt.org/"))) {
            iriString = iriString.replace("http://hdt.org/", "");
            if (iriString.startsWith("SO")) {
                id = Long.parseLong(iriString.substring(2));
                position = SimpleIRIHDT.SHARED_POS;
            } else if (iriString.startsWith("S")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.SUBJECT_POS;
            } else if (iriString.startsWith("P")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.PREDICATE_POS;
            } else if (iriString.startsWith("O")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.OBJECT_POS;
            }
            return new SimpleIRIHDT(this.hdt, position, id);
        }
        return subj;
    }

    public IRI getIRIHdtPred(IRI pred) {
        String iriString = pred.toString();
        if (iriString.startsWith(("http://hdt.org/"))) {
            long id = -1;
            int position = -1;
            iriString = iriString.replace("http://hdt.org/", "");
            if (iriString.startsWith("P")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.PREDICATE_POS;
            } else if (iriString.startsWith("SO")) {
                id = Long.parseLong(iriString.substring(2));
                position = SimpleIRIHDT.SHARED_POS;
            } else if (iriString.startsWith("S")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.SUBJECT_POS;
            } else if (iriString.startsWith("O")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.OBJECT_POS;
            }
            return new SimpleIRIHDT(this.hdt, position, id);
        }
        return pred;
    }

    public Value getIRIHdtObj(Value object) {
        String iriString = object.toString();
        long id = -1;
        int position = -1;
        if (iriString.startsWith(("http://hdt.org/"))) {
            iriString = iriString.replace("http://hdt.org/", "");
            if (iriString.startsWith("SO")) {
                id = Long.parseLong(iriString.substring(2));
                position = SimpleIRIHDT.SHARED_POS;
            } else if (iriString.startsWith("O")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.OBJECT_POS;
            } else if (iriString.startsWith("P")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.PREDICATE_POS;
            } else if (iriString.startsWith("S")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.SUBJECT_POS;
            }
            return new SimpleIRIHDT(this.hdt, position, id);
        }
        return object;
    }

    public long convertSubj(Resource subj) {
        if (subj != null) {
            // if it is a HDT IRI we do not need to make a full conversion, we already have the IDs
            if (subj instanceof SimpleIRIHDT) {
                long id = ((SimpleIRIHDT) subj).getId();
                long position = ((SimpleIRIHDT) subj).getPostion();
                if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.SUBJECT_POS) {
                    return id;
                }
                String translate = "";
                if (position == SimpleIRIHDT.PREDICATE_POS) {
                    translate =
                            hdt.getDictionary()
                                    .idToString(id,
                                            TripleComponentRole.PREDICATE)
                                    .toString();
                } else if (position == SimpleIRIHDT.OBJECT_POS) {
                    translate =
                            hdt.getDictionary()
                                    .idToString(id,
                                            TripleComponentRole.OBJECT)
                                    .toString();
                }
                id = hdt.getDictionary().stringToId(translate, TripleComponentRole.SUBJECT);
                return id;
            } else {
                long id = this.hdt.getDictionary().stringToId(subj.toString(), TripleComponentRole.SUBJECT);
                return id;
            }
        } else {
            return 0;
        }
    }

    public long convertPred(IRI pred) {
        if (pred != null) {
            // if it is a HDT IRI we do not need to make a full conversion, we already have the IDs
            if (pred instanceof SimpleIRIHDT) {
                long id = ((SimpleIRIHDT) pred).getId();
                long position = ((SimpleIRIHDT) pred).getPostion();
                if (position == SimpleIRIHDT.PREDICATE_POS) {
                    return id;
                }
                String translate = "";
                if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.SUBJECT_POS) {
                    translate =
                            hdt.getDictionary()
                                    .idToString(id,
                                            TripleComponentRole.SUBJECT)
                                    .toString();
                } else if (position == SimpleIRIHDT.OBJECT_POS) {
                    translate =
                            hdt.getDictionary()
                                    .idToString(id,
                                            TripleComponentRole.OBJECT)
                                    .toString();
                }
                return hdt.getDictionary().stringToId(translate, TripleComponentRole.PREDICATE);

            } else {
                return this.hdt.getDictionary().stringToId(pred.toString(), TripleComponentRole.PREDICATE);
            }

        } else {
            return 0;
        }
    }


    public long convertObj(Value obj) {
        if (obj != null) {
            // if it is a HDT IRI we do not need to make a full conversion, we already have the IDs
            if (obj instanceof SimpleIRIHDT) {
                long id = ((SimpleIRIHDT) obj).getId();
                int position = ((SimpleIRIHDT) obj).getPostion();
                if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.OBJECT_POS) {
                    return id;
                }
                String translate = "";
                if (position == SimpleIRIHDT.PREDICATE_POS) {
                    translate =
                            hdt.getDictionary()
                                    .idToString(id,
                                            TripleComponentRole.PREDICATE)
                                    .toString();
                } else if (position == SimpleIRIHDT.SUBJECT_POS) {
                    translate =
                            hdt.getDictionary()
                                    .idToString(id,
                                            TripleComponentRole.SUBJECT)
                                    .toString();
                }
                return hdt.getDictionary().stringToId(translate, TripleComponentRole.OBJECT);
            } else {
                return this.hdt.getDictionary().stringToId(obj.toString(), TripleComponentRole.OBJECT);
            }
        } else {
            return 0;
        }
    }


    public IRI subjectIdToIRI(long id){
        if (id <= this.hdt.getDictionary().getNshared()) {
            return valueFactory.createIRI("http://hdt.org/SO"+id);
        } else {
            return valueFactory.createIRI("http://hdt.org/S" + id);
        }
    }

    public IRI predicateIdToIRI(long id){
        return valueFactory.createIRI("http://hdt.org/P" + id);
    }

    public IRI objectIdToIRI(long id){
        if (id <= this.hdt.getDictionary().getNshared()) {
            return valueFactory.createIRI("http://hdt.org/SO"+id);
        } else {
            return valueFactory.createIRI("http://hdt.org/O" + id);
        }
    }
}

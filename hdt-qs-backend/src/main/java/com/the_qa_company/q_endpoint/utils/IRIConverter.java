package com.the_qa_company.q_endpoint.utils;

import com.the_qa_company.q_endpoint.model.AbstractValueFactoryHDT;
import com.the_qa_company.q_endpoint.model.SimpleIRIHDT;
import com.the_qa_company.q_endpoint.model.SimpleLiteralHDT;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IRIConverter {
    private static final Logger logger = LoggerFactory.getLogger(IRIConverter.class);
    private HDT hdt;
    private ValueFactory valueFactory;
    private ValueFactory tempFactory;

    public IRIConverter(HDT hdt) {
        this.hdt = hdt;
        this.valueFactory = new AbstractValueFactoryHDT(this.hdt);
        this.tempFactory = new MemValueFactory();
    }

    public Resource getIRIHdtSubj(Resource subj) {
        String iriString = subj.toString();
        long id = -1;
        int position = -1;
        if (subj.isBNode()) {
            return this.valueFactory.createBNode(iriString);
        } else if (iriString.startsWith(("http://hdt.org/"))) {
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
        } else { // string was not converted upon insert - iriString the real IRI
            return new SimpleIRIHDT(this.hdt, subj.toString());
        }
    }

    public Value getIRIHdtPred(IRI pred) {
        String iriString = pred.toString();
        long id = -1;
        int position = -1;
        if (pred.isBNode()) {
            return this.valueFactory.createBNode(iriString);
        } else if (iriString.startsWith(("http://hdt.org/"))) {
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
        } else { // string was not converted upon insert - iriString the real IRI
            return new SimpleIRIHDT(this.hdt, pred.toString());
        }
    }

    public Value getIRIHdtObj(Value object) {
        String iriString = object.toString();
        long id = -1;
        int position = -1;
        if (object.isBNode()) {
            return this.valueFactory.createBNode(iriString);
        } else if (iriString.startsWith(("http://hdt.org/"))) {
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
            if (isLiteral(id)) {
                return new SimpleLiteralHDT(this.hdt, id, this.valueFactory);
            } else {
                return new SimpleIRIHDT(this.hdt, position, id);
            }
        } else { // string was not converted upon insert - iriString the real IRI
            if (object.isLiteral())
                return object;
            else
                return new SimpleIRIHDT(this.hdt, object.toString());
        }
    }

    public Resource convertSubj(Resource subj) {
        Resource newSubj = null;
        boolean convert = false;
        if (subj != null) {
            if (subj.isBNode()) {
                return subj;
            } else if (subj instanceof SimpleIRIHDT) {
                long id = ((SimpleIRIHDT) subj).getId();
                long position = ((SimpleIRIHDT) subj).getPostion();
                if (id != -1) {
                    String prefix = "http://hdt.org/";
                    if (position == SimpleIRIHDT.PREDICATE_POS) {
                        String translate =
                                hdt.getDictionary()
                                        .idToString(id,
                                                TripleComponentRole.PREDICATE)
                                        .toString();
                        id = hdt.getDictionary().stringToId(translate, TripleComponentRole.SUBJECT);
                    } else if (position == SimpleIRIHDT.OBJECT_POS) {
                        String translate =
                                hdt.getDictionary()
                                        .idToString(id,
                                                TripleComponentRole.OBJECT)
                                        .toString();
                        id = hdt.getDictionary().stringToId(translate, TripleComponentRole.SUBJECT);
                    }
                    if (id == -1) { // means that id doesn't exist after translation, return with id = -1
                        //newSubj = new SimpleIRIHDT(hdt,prefix+"empty");;
                        newSubj = subj;
                    } else {
                        if (id <= this.hdt.getDictionary().getNshared()) {
                            prefix += "SO";
                            String subjIdentifier = prefix + id;
                            newSubj = new SimpleIRIHDT(hdt, subjIdentifier, SimpleIRIHDT.SHARED_POS, id);
                        } else {
                            prefix += "S";
                            String subjIdentifier = prefix + id;
                            newSubj = new SimpleIRIHDT(hdt, subjIdentifier, SimpleIRIHDT.SUBJECT_POS, id);
                        }
                    }
                } else {
                    convert = true;
                }
            } else { // upon insertion need to convert string to ID
                convert = true;
            }
            if (convert) {
                String subjStr = subj.toString();
                long subjId = this.hdt.getDictionary().stringToId(subjStr, TripleComponentRole.SUBJECT);
                if (subjId != -1) {
                    if (subjId <= this.hdt.getDictionary().getNshared()) {
                        newSubj = new SimpleIRIHDT(hdt, "http://hdt.org/SO" + subjId, SimpleIRIHDT.SHARED_POS, subjId);
                    } else {
                        newSubj = new SimpleIRIHDT(hdt, "http://hdt.org/S" + subjId, SimpleIRIHDT.SUBJECT_POS, subjId);
                    }
                } else {
                    newSubj = subj;
                }
            }
        }
        return newSubj;
    }

    public IRI convertPred(IRI pred) {
        IRI newPred = null;
        boolean convert = false;
        if (pred != null) {
            if (pred.isBNode()) {
                return pred;
            } else if (pred instanceof SimpleIRIHDT) {
                long id = ((SimpleIRIHDT) pred).getId();
                long position = ((SimpleIRIHDT) pred).getPostion();
                if (id != -1) {
                    String prefix = "http://hdt.org/";
                    prefix += "P";
                    String translate = "";
                    if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.SUBJECT_POS) {
                        translate =
                                hdt.getDictionary()
                                        .idToString(id,
                                                TripleComponentRole.SUBJECT)
                                        .toString();
                        id = hdt.getDictionary().stringToId(translate, TripleComponentRole.PREDICATE);
                    } else if (position == SimpleIRIHDT.OBJECT_POS) {
                        translate =
                                hdt.getDictionary()
                                        .idToString(id,
                                                TripleComponentRole.OBJECT)
                                        .toString();
                        id = hdt.getDictionary().stringToId(translate, TripleComponentRole.PREDICATE);
                    }
                    String predIdentifier = prefix + id;
                    if (id == -1) {
                        //newPred = new SimpleIRIHDT(hdt,prefix+"empty");
                        // newPred = pred;
                        newPred = this.tempFactory.createIRI(translate);
                    } else
                        newPred = new SimpleIRIHDT(hdt, predIdentifier, SimpleIRIHDT.PREDICATE_POS, id);
                } else {
                    convert = true;
                }
            } else {
                convert = true;
            }
            if (convert) {
                String predStr = pred.toString();
                long predId = this.hdt.getDictionary().stringToId(predStr, TripleComponentRole.PREDICATE);
                if (predId != -1) {
                    newPred = new SimpleIRIHDT(hdt, "http://hdt.org/P" + predId, SimpleIRIHDT.PREDICATE_POS, predId);
                } else {
                    newPred = pred;
                }
            }
        }
        return newPred;
    }

    public Value convertObj(Value obj) {
        Value newObj = null;
        boolean convert = false;
        if (obj != null) {
            if (obj.isBNode()) {
                return obj;
            } else if (obj instanceof SimpleIRIHDT) {
                long id = ((SimpleIRIHDT) obj).getId();
                long position = ((SimpleIRIHDT) obj).getPostion();
                if (id != -1) {
                    String prefix = "http://hdt.org/";
                    if (position == SimpleIRIHDT.SUBJECT_POS || position == SimpleIRIHDT.SHARED_POS) {
                        String translate =
                                hdt.getDictionary()
                                        .idToString(id,
                                                TripleComponentRole.SUBJECT)
                                        .toString();
                        id = hdt.getDictionary().stringToId(translate, TripleComponentRole.OBJECT);
                    } else if (position == SimpleIRIHDT.PREDICATE_POS) {
                        String translate =
                                hdt.getDictionary()
                                        .idToString(id,
                                                TripleComponentRole.PREDICATE)
                                        .toString();
                        id = hdt.getDictionary().stringToId(translate, TripleComponentRole.OBJECT);
                    }
                    if (id == -1) {
                        // newObj = new SimpleIRIHDT(hdt,prefix+"empty");
                        newObj = obj;
                    } else {
                        if (id <= this.hdt.getDictionary().getNshared()) {
                            prefix += "SO";
                            String objIdentifier = prefix + id;
                            newObj = new SimpleIRIHDT(hdt, objIdentifier, SimpleIRIHDT.SHARED_POS, id);
                        } else {
                            prefix += "O";
                            String objIdentifier = prefix + id;
                            newObj = new SimpleIRIHDT(hdt, objIdentifier, SimpleIRIHDT.OBJECT_POS, id);
                        }
                    }
                } else {
                    convert = true;
                }
            } else if (obj instanceof SimpleLiteralHDT) {
                long objId = ((SimpleLiteralHDT) obj).getHdtID();
                newObj = new SimpleIRIHDT(hdt, "http://hdt.org/O" + objId, SimpleIRIHDT.OBJECT_POS, objId);
            } else {
                convert = true;
            }
            if (convert) {
                String objStr = obj.toString();
                long objId = this.hdt.getDictionary().stringToId(objStr, TripleComponentRole.OBJECT);
                if (objId != -1) {
                    if (objId <= this.hdt.getDictionary().getNshared()) {

                        newObj = new SimpleIRIHDT(hdt, "http://hdt.org/SO" + objId, SimpleIRIHDT.SHARED_POS, objId);
                    } else {
                        newObj = new SimpleIRIHDT(hdt, "http://hdt.org/O" + objId, SimpleIRIHDT.OBJECT_POS, objId);
                    }
                } else {
                    newObj = obj;
                }
            }
        }
        return newObj;
    }

    //    public Literal convertLiteralToIRIHDT(){
//        return null;
//    }
//    public Literal convertLiteral(Literal obj){
//        String objStr = obj.toString();
//        if(objStr.startsWith("http://hdt.org/")){
//            objStr = objStr.replace("http://hdt.org/","");
//            long id = Long.parseLong(objStr.substring(1));
//            return new SimpleLiteralHDT(hdt,id,this.valueFactory);
//        }else{
//            long objId = this.hdt.getDictionary().stringToId(objStr, TripleComponentRole.OBJECT);
//            if(objId != -1){
//                // found literal in HDT then return a literal object
//                return new SimpleLiteralHDT(hdt,objId,this.valueFactory);
//            }else {
//                return obj;
//            }
//        }
//    }
    private boolean isLiteral(long id) {
        //MultipleBaseDictionary dictionary = (;
        String dataType = this.hdt.getDictionary().dataTypeOfId(id);
        if (dataType.equals("")) {
            logger.info("Given id is not available in the dictionary literals");
            return false;
        }
        return !dataType.equals("NO_DATATYPE") && !dataType.equals("section");
    }
}

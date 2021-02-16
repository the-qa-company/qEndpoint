package org.rdfhdt.hdt.rdf4j.utility;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.AbstractValueFactoryHDT;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

public class IRIConverter {
    private HDT hdt;
    private ValueFactory valueFactory;
    private ValueFactory tempFactory;
    public IRIConverter(HDT hdt){
        this.hdt = hdt;
        this.valueFactory = new AbstractValueFactoryHDT(this.hdt);
        this.tempFactory = new MemValueFactory();
    }
    public SimpleIRIHDT getIRIHdtSubj(Resource subj){
        String iriString = subj.toString();
        long id = -1;
        int position = -1;
        if(iriString.startsWith(("http://hdt.org/"))){
            iriString = iriString.replace("http://hdt.org/","");
            if(iriString.startsWith("SO")){
                id = Long.parseLong(iriString.substring(2));
                position = SimpleIRIHDT.SHARED_POS;
            }else if(iriString.startsWith("S")){
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.SUBJECT_POS;
            }else if(iriString.startsWith("P")){
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.PREDICATE_POS;
            }
            return new SimpleIRIHDT(this.hdt,position,id);
        }else{ // string was not converted upon insert - iriString the real IRI
            return new SimpleIRIHDT(this.hdt,iriString);
        }
    }
    public SimpleIRIHDT getIRIHdtPred(IRI pred){
        String iriString = pred.toString();
        long id = -1;
        int position = -1;
        if(iriString.startsWith(("http://hdt.org/"))){
            iriString = iriString.replace("http://hdt.org/","");
            if(iriString.startsWith("P")) {
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.PREDICATE_POS;
            }else if(iriString.startsWith("SO")){
                id = Long.parseLong(iriString.substring(2));
                position = SimpleIRIHDT.SHARED_POS;
            }else if(iriString.startsWith("S")){
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.SUBJECT_POS;
            }else if(iriString.startsWith("O")){
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.OBJECT_POS;
            }
            return new SimpleIRIHDT(this.hdt,position,id);
        }else{ // string was not converted upon insert - iriString the real IRI
            return new SimpleIRIHDT(this.hdt,iriString);
        }
    }
    public SimpleIRIHDT getIRIHdtObj(Value object){
        String iriString = object.toString();
        long id = -1;
        int position = -1;
        if(iriString.startsWith(("http://hdt.org/"))){
            iriString = iriString.replace("http://hdt.org/","");
            if(iriString.startsWith("SO")){
                id = Long.parseLong(iriString.substring(2));
                position = SimpleIRIHDT.SHARED_POS;
            }else if(iriString.startsWith("O")){
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.OBJECT_POS;
            }else if(iriString.startsWith("P")){
                id = Long.parseLong(iriString.substring(1));
                position = SimpleIRIHDT.PREDICATE_POS;
            }
            return new SimpleIRIHDT(this.hdt,position,id);
        }else{ // string was not converted upon insert - iriString the real IRI
            return new SimpleIRIHDT(this.hdt,iriString);
        }
    }


    public Resource convertSubj(Resource subj){
        Resource newSubj = null;
        if(subj != null) {
            if (subj instanceof SimpleIRIHDT) {
                long id = ((SimpleIRIHDT) subj).getId();
                long position = ((SimpleIRIHDT) subj).getPostion();
                if (id != -1) {
                    String prefix = "http://hdt.org/";
                    if (position == SimpleIRIHDT.SHARED_POS) {
                        prefix += "SO";
                    } else if(position == SimpleIRIHDT.SUBJECT_POS){
                        prefix += "S";
                    }else if(position == SimpleIRIHDT.PREDICATE_POS){
                        prefix += "P";
                    }
                    String subjIdentifier = prefix + id;
                    newSubj = this.tempFactory.createIRI(subjIdentifier);
                } else {
                    newSubj = subj;
                }
            } else { // upon insertion need to convert string to ID
                String subjStr = subj.toString();
                long subjId = this.hdt.getDictionary().stringToId(subjStr, TripleComponentRole.SUBJECT);
                if (subjId != -1) {
                    if (subjId <= this.hdt.getDictionary().getNshared()) {
                        newSubj = this.valueFactory.createIRI("http://hdt.org/SO" + subjId);
                    } else {
                        newSubj = this.valueFactory.createIRI("http://hdt.org/S" + subjId);
                    }
                } else {
                    newSubj = subj;
                }
            }
        }
        return newSubj;
    }
    public IRI convertPred(IRI pred){
        IRI newPred = null;
        if(pred != null) {
            if (pred instanceof SimpleIRIHDT) {
                long id = ((SimpleIRIHDT) pred).getId();
                long position = ((SimpleIRIHDT) pred).getPostion();
                if (id != -1) {
                    String prefix = "http://hdt.org/";
                    if (position == SimpleIRIHDT.SHARED_POS) {
                        prefix += "SO";
                    } else if(position == SimpleIRIHDT.SUBJECT_POS){
                        prefix += "S";
                    }else if (position == SimpleIRIHDT.OBJECT_POS) {
                        prefix += "O";
                    } else if(position == SimpleIRIHDT.PREDICATE_POS){
                        prefix += "P";
                    }
                    String predIdentifier = prefix + id;
                    newPred = this.tempFactory.createIRI(predIdentifier);
                } else {
                    newPred = pred;
                }
            } else {
                String predStr = pred.toString();
                long predId = this.hdt.getDictionary().stringToId(predStr, TripleComponentRole.PREDICATE);
                if (predId != -1) {
                    newPred = this.valueFactory.createIRI("http://hdt.org/P" + predId);
                } else {
                    newPred = pred;
                }
            }
        }
        return newPred;
    }
    public Value convertObj(Value obj){
        Value newObj = null;
        if(obj != null) {
            if (obj instanceof SimpleIRIHDT) {
                long id = ((SimpleIRIHDT) obj).getId();
                long position = ((SimpleIRIHDT) obj).getPostion();
                if (id != -1) {
                    String prefix = "http://hdt.org/";
                    if (position == SimpleIRIHDT.SHARED_POS) {
                        prefix += "SO";
                    } else if(position == SimpleIRIHDT.SUBJECT_POS){
                        prefix += "S";
                    }else if (position == SimpleIRIHDT.OBJECT_POS) {
                        prefix += "O";
                    } else if(position == SimpleIRIHDT.PREDICATE_POS){
                        prefix += "P";
                    }
                    String objIdentifier = prefix + id;
                    newObj = this.tempFactory.createIRI(objIdentifier);
                } else {
                    newObj = obj;
                }
            } else {
                String objStr = obj.toString();
                long objId = this.hdt.getDictionary().stringToId(objStr, TripleComponentRole.OBJECT);
                if (objId != -1) {
                    if (objId <= this.hdt.getDictionary().getNshared()) {
                        newObj = this.valueFactory.createIRI("http://hdt.org/SO" + objId);
                    } else {
                        newObj = this.valueFactory.createIRI("http://hdt.org/O" + objId);
                    }
                } else {
                    newObj = obj;
                }
            }
        }
        return newObj;
    }
}

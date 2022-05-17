package com.the_qa_company.qendpoint.model;

import com.the_qa_company.qendpoint.controller.Sparql;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.base.AbstractBNode;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

public class SimpleBNodeHDT extends AbstractBNode {
	private final HDT hdt;
	private final int position;
	private final long id;
	private String idValue;

	public SimpleBNodeHDT(HDT hdt, int position, long id) {
		if (id == -1) {
			System.out.println("This should not happen");
		}
		this.hdt = hdt;
		this.position = position;
		this.id = id;
	}

	@Override
	public String getID() {
		if (idValue == null) {
			Sparql.count++;
			if (this.position == SimpleIRIHDT.SHARED_POS) {
				idValue = hdt.getDictionary().idToString(this.id, TripleComponentRole.SUBJECT).toString();
			} else if (this.position == SimpleIRIHDT.SUBJECT_POS) {
				idValue = hdt.getDictionary().idToString(this.id, TripleComponentRole.SUBJECT).toString();
			} else if (this.position == SimpleIRIHDT.OBJECT_POS) {
				idValue = hdt.getDictionary().idToString(this.id, TripleComponentRole.OBJECT).toString();
			} else if (this.position == SimpleIRIHDT.PREDICATE_POS) {
				idValue = hdt.getDictionary().idToString(this.id, TripleComponentRole.PREDICATE).toString();
			} else {
				throw new HDTLoadException("bad position: " + position);
			}
			idValue = idValue.substring(2);
		}
		return idValue;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (this == o) {
			return true;
		} else if (o instanceof SimpleBNodeHDT && this.id != -1 && ((SimpleBNodeHDT) o).getHdtId() != -1) {
			return this.id == (((SimpleBNodeHDT) o).getHdtId());
		} else { // could not compare IDs, we have to compare to string
			if (!(o instanceof BNode)) {
				return false;
			}
			return getID().equals(((BNode) o).getID());
		}
	}

	@Override
	public int hashCode() {
		if (id != -1) {
			String prefix = "http://hdt.org/";
			if (this.position == SimpleIRIHDT.SHARED_POS)
				prefix += "SO";
			else if (this.position == SimpleIRIHDT.SUBJECT_POS)
				prefix += "S";
			else if (this.position == SimpleIRIHDT.PREDICATE_POS)
				prefix += "P";
			else if (this.position == SimpleIRIHDT.OBJECT_POS)
				prefix += "O";
			else {
				if (idValue != null)
					prefix = idValue;
				return prefix.hashCode();
			}
			prefix += id;
			return prefix.hashCode();
		} else {
			return toString().hashCode();
		}
	}

	public long getHdtId() {
		return id;
	}
}

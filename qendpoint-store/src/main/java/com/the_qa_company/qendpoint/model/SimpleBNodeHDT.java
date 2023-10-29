package com.the_qa_company.qendpoint.model;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.base.AbstractBNode;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;

public class SimpleBNodeHDT extends AbstractBNode implements HDTValue {
	private final HDT hdt;
	private final int position;
	private final long id;
	private String idValue;
	private boolean delegate;

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
		} else if (o instanceof HDTValue hv && this.id != -1 && hv.getHDTId() != -1) {
			return this.id == hv.getHDTId();
		} else { // could not compare IDs, we have to compare to string
			if (!(o instanceof BNode)) {
				return false;
			}
			return getID().equals(((BNode) o).getID());
		}
	}

	@Override
	public int hashCode() {
		if (id != -1 && !delegate) {
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

	@Override
	public long getHDTId() {
		return id;
	}

	@Override
	public int getHDTPosition() {
		return position;
	}

	@Override
	public void setDelegate(boolean delegate) {
		this.delegate = delegate;
	}

	@Override
	public boolean isDelegate() {
		return delegate;
	}
}

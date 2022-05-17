package com.the_qa_company.qendpoint.model;

import com.the_qa_company.qendpoint.controller.Sparql;

import org.eclipse.rdf4j.model.base.AbstractIRI;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

public class SimpleIRIHDT extends AbstractIRI {

	private static final long serialVersionUID = -3220264926968931192L;
	public static final byte SUBJECT_POS = 1;
	public static final byte PREDICATE_POS = 2;
	public static final byte OBJECT_POS = 3;
	public static final byte SHARED_POS = 4;

	private final HDT hdt;
	private int postion;
	private long id;
	private String iriString;
	// An index indicating the first character of the local name in the IRI
	// string, -1 if not yet set.
	private int localNameIdx;

	public SimpleIRIHDT(HDT hdt, int position, long id) {
		if (id == -1) {
			System.out.println("This should not happen");
		}
		this.hdt = hdt;
		this.postion = position;
		this.id = id;
		this.localNameIdx = -1;
	}

	public SimpleIRIHDT(HDT hdt, String iriString) {
		this.hdt = hdt;
		this.iriString = iriString;
		this.id = -1;
		this.localNameIdx = -1;
	}

	public long getId() {
		return id;
	}

	public int getPostion() {
		return postion;
	}

	@Override
	public String toString() {
		if (iriString == null) {
			iriString = stringValue();
		}
		return iriString;
	}

	@Override
	public String stringValue() {
		if (this.iriString != null) {
			return this.iriString;
		} else {
			Sparql.count++;

			if (this.postion == SHARED_POS) {
				return hdt.getDictionary().idToString(this.id, TripleComponentRole.SUBJECT).toString();
			} else if (this.postion == SUBJECT_POS) {
				return hdt.getDictionary().idToString(this.id, TripleComponentRole.SUBJECT).toString();
			} else if (this.postion == OBJECT_POS) {
				CharSequence charSequence = hdt.getDictionary().idToString(this.id, TripleComponentRole.OBJECT);
				if (charSequence == null) {
					throw new NullPointerException("NULL for ID: " + id);
				}
				return charSequence.toString();
			} else if (this.postion == PREDICATE_POS) {
				CharSequence charSequence = hdt.getDictionary().idToString(this.id, TripleComponentRole.PREDICATE);
				if (charSequence == null) {
					throw new NullPointerException("NULL for ID: " + id);
				}
				return charSequence.toString();
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

	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (this == o) {
			return true;
		} else if (o instanceof SimpleIRIHDT && this.id != -1 && ((SimpleIRIHDT) o).getId() != -1) {
			return this.id == (((SimpleIRIHDT) o).getId());
		} else { // could not compare IDs, we have to compare to string
			return toString().equals(o.toString());
		}
	}

	@Override
	public int hashCode() {
		if (id != -1) {
			String prefix = "http://hdt.org/";
			if (this.postion == SHARED_POS)
				prefix += "SO";
			else if (this.postion == SUBJECT_POS)
				prefix += "S";
			else if (this.postion == PREDICATE_POS)
				prefix += "P";
			else if (this.postion == OBJECT_POS)
				prefix += "O";
			else {
				if (iriString != null)
					prefix = iriString;
				return prefix.hashCode();
			}
			prefix += id;
			return prefix.hashCode();
		} else {
			return toString().hashCode();
		}
	}

	public String getIriString() {
		return iriString;
	}

	public void convertToNonHDTIRI() {
		if (iriString == null) {
			iriString = stringValue();
		}
		this.id = -1;
	}
}

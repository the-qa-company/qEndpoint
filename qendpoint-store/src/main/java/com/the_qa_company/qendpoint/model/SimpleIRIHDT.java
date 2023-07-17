package com.the_qa_company.qendpoint.model;

import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.store.exception.EndpointStoreException;
import org.eclipse.rdf4j.model.base.AbstractIRI;
import org.eclipse.rdf4j.model.util.URIUtil;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;

import java.io.Serial;

public class SimpleIRIHDT extends AbstractIRI implements HDTValue {

	@Serial
	private static final long serialVersionUID = -3220264926968931192L;
	public static final byte SUBJECT_POS = 1;
	public static final byte PREDICATE_POS = 2;
	public static final byte OBJECT_POS = 3;
	public static final byte SHARED_POS = 4;

	public static byte getPos(DictionarySectionRole role) {
		return switch (role) {
		case SHARED -> SHARED_POS;
		case SUBJECT -> SUBJECT_POS;
		case PREDICATE -> PREDICATE_POS;
		case OBJECT -> OBJECT_POS;
		};
	}

	private final HDT hdt;
	private int postion;
	private long id;
	private String iriString;
	// An index indicating the first character of the local name in the IRI
	// string, -1 if not yet set.
	private int localNameIdx;
	private boolean delegate;

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
			CharSequence charSequence;
			if (this.postion == SHARED_POS || this.postion == SUBJECT_POS) {
				charSequence = hdt.getDictionary().idToString(this.id, TripleComponentRole.SUBJECT);
			} else if (this.postion == OBJECT_POS) {
				charSequence = hdt.getDictionary().idToString(this.id, TripleComponentRole.OBJECT);
			} else if (this.postion == PREDICATE_POS) {
				charSequence = hdt.getDictionary().idToString(this.id, TripleComponentRole.PREDICATE);
			} else {
				throw new EndpointStoreException("bad postion value: " + postion);
			}

			if (charSequence == null) {
				throw new EndpointStoreException("Can't find HDT ID: " + id);
			}

			return charSequence.toString();
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
		if (id != -1 && !delegate) {
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

	@Override
	public void setDelegate(boolean delegate) {
		this.delegate = delegate;
	}

	@Override
	public boolean isDelegate() {
		return delegate;
	}
}

package com.the_qa_company.qendpoint.model;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.store.exception.EndpointStoreException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.io.Serial;

public class SimpleIRIHDT implements HDTValue, IRI {

	@Serial
	private static final long serialVersionUID = -3220264926968931192L;
	public static final byte SUBJECT_POS = 1;
	public static final byte PREDICATE_POS = 2;
	public static final byte OBJECT_POS = 3;
	public static final byte GRAPH_POS = 4;
	public static final byte SHARED_POS = 5;

	public static byte getPos(DictionarySectionRole role) {
		return switch (role) {
		case SHARED -> SHARED_POS;
		case SUBJECT -> SUBJECT_POS;
		case PREDICATE -> PREDICATE_POS;
		case OBJECT -> OBJECT_POS;
		case GRAPH -> throw new NotImplementedException("TODO: GRAPH");
		};
	}

	private final Dictionary dict;
	private final int position;
	private long id;

	private IRI delegate;

	public SimpleIRIHDT(Dictionary dict, int position, long id) {
		this.dict = dict;
		this.position = position;
		this.id = id;
		// if (!(id > 0 && position >= SUBJECT_POS && position <= SHARED_POS)) {
//			throw new IllegalArgumentException("Bad argument %d > 0 / pos = %d".formatted(id, position));
//		}
	}

	@Override
	public long getHDTId() {
		return id;
	}

	@Override
	public int getHDTPosition() {
		return position;
	}

	public long getId() {
		return id;
	}

	private IRI getIRI() {
		if (delegate == null) {
			CharSequence charSequence;
			if (this.position == SHARED_POS || this.position == SUBJECT_POS) {
				charSequence = dict.idToString(this.id, TripleComponentRole.SUBJECT);
			} else if (this.position == OBJECT_POS) {
				charSequence = dict.idToString(this.id, TripleComponentRole.OBJECT);
			} else if (this.position == PREDICATE_POS) {
				charSequence = dict.idToString(this.id, TripleComponentRole.PREDICATE);
			} else if (this.position == GRAPH_POS) {
				charSequence = dict.idToString(this.id, TripleComponentRole.GRAPH);
			} else {
				throw new EndpointStoreException("bad postion value: " + position);
			}

			if (charSequence == null) {
				throw new EndpointStoreException("Can't find HDT ID: " + id);
			}

			delegate = SimpleValueFactory.getInstance().createIRI(charSequence.toString());
		}
		return delegate;
	}

	@Override
	public String toString() {
		return getIRI().toString();
	}

	@Override
	public String stringValue() {
		return getIRI().stringValue();

	}

	public String getNamespace() {
		return getIRI().getNamespace();

	}

	public String getLocalName() {
		return getIRI().getLocalName();

	}

	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (this == o) {
			return true;
		} else if (o instanceof SimpleIRIHDT && this.id != -1 && ((SimpleIRIHDT) o).getId() != -1) {
			return this.id == (((SimpleIRIHDT) o).getId());
		} else { // could not compare IDs, we have to compare to string
			if (o instanceof IRI) {
				return toString().equals(o.toString());
			} else {
				return false;
			}

		}
	}

	@Override
	public int hashCode() {
		return getIRI().hashCode();
	}

	public void convertToNonHDTIRI() {

		this.id = -1;
	}

	@Override
	public void setDelegate(boolean delegate) {
		if (delegate) {
			getIRI();
		}
	}

	@Override
	public boolean isDelegate() {
		return delegate != null;
	}

}

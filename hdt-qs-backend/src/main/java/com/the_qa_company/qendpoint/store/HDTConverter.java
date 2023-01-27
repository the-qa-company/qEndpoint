package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.model.HDTValue;
import com.the_qa_company.qendpoint.model.SimpleBNodeHDT;
import com.the_qa_company.qendpoint.model.SimpleIRIHDT;
import com.the_qa_company.qendpoint.model.SimpleLiteralHDT;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;

// there are 4 types of resources:
// resources coming from outside,
// HDT IDs
// resources with HDT IDs used inside rdf4j NOTE: it is not possible to use HDT IDs in RDF4j since these are converted internally to NativeStoreIRIs and the ID is lost
// and resources with HDT IDs (SimpleHDTIRI) used to output
//
// this class makes the conversion between the different types resources
public class HDTConverter {
	private final EndpointStore endpoint;
	private final HDT hdt;
	private final ValueFactory valueFactory = new MemValueFactory();
	private final boolean optimizeDatatype;

	public HDTConverter(EndpointStore endpoint) {
		this.endpoint = endpoint;
		this.hdt = endpoint.getHdt();
		this.optimizeDatatype = false; // hdt.getDictionary() instanceof
										// MultipleSectionDictionary;
	}

	// method to get the ID of a resource
	public long subjectToID(Resource subj) {
		if (subj == null) {
			return 0;
		}
		if (!(subj instanceof SimpleIRIHDT)) {
			return this.hdt.getDictionary().stringToId(subj.toString(), TripleComponentRole.SUBJECT);
		}
		// if it is a HDT IRI we do not need to make a full conversion, we
		// already have the IDs
		long id = ((SimpleIRIHDT) subj).getId();
		long position = ((SimpleIRIHDT) subj).getPostion();
		if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.SUBJECT_POS) {
			return id;
		}
		String translate = "";
		if (position == SimpleIRIHDT.PREDICATE_POS) {
			translate = hdt.getDictionary().idToString(id, TripleComponentRole.PREDICATE).toString();
		} else if (position == SimpleIRIHDT.OBJECT_POS) {
			translate = hdt.getDictionary().idToString(id, TripleComponentRole.OBJECT).toString();
		}
		id = hdt.getDictionary().stringToId(translate, TripleComponentRole.SUBJECT);
		return id;
	}

	public long predicateToID(IRI pred) {
		if (pred != null) {
			// if it is a HDT IRI we do not need to make a full conversion, we
			// already have the IDs
			if (pred instanceof SimpleIRIHDT) {
				long id = ((SimpleIRIHDT) pred).getId();
				long position = ((SimpleIRIHDT) pred).getPostion();
				if (position == SimpleIRIHDT.PREDICATE_POS) {
					return id;
				}
				String translate = "";
				if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.SUBJECT_POS) {
					translate = hdt.getDictionary().idToString(id, TripleComponentRole.SUBJECT).toString();
				} else if (position == SimpleIRIHDT.OBJECT_POS) {
					translate = hdt.getDictionary().idToString(id, TripleComponentRole.OBJECT).toString();
				}
				return hdt.getDictionary().stringToId(translate, TripleComponentRole.PREDICATE);

			} else {
				return this.hdt.getDictionary().stringToId(pred.toString(), TripleComponentRole.PREDICATE);
			}

		} else {
			return 0;
		}
	}

	public long objectToID(Value obj) {
		if (obj != null) {
			// if it is a HDT IRI we do not need to make a full conversion, we
			// already have the IDs
			if (obj instanceof SimpleIRIHDT) {
				long id = ((SimpleIRIHDT) obj).getId();
				int position = ((SimpleIRIHDT) obj).getPostion();
				if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.OBJECT_POS) {
					return id;
				}
				String translate = "";
				if (position == SimpleIRIHDT.PREDICATE_POS) {
					translate = hdt.getDictionary().idToString(id, TripleComponentRole.PREDICATE).toString();
				} else if (position == SimpleIRIHDT.SUBJECT_POS) {
					translate = hdt.getDictionary().idToString(id, TripleComponentRole.SUBJECT).toString();
				}
				return hdt.getDictionary().stringToId(translate, TripleComponentRole.OBJECT);
			} else if (obj instanceof SimpleLiteralHDT) {
				return ((SimpleLiteralHDT) obj).getHdtID();
			} else {
				return this.hdt.getDictionary().stringToId(obj.toString(), TripleComponentRole.OBJECT);
			}
		} else {
			return 0;
		}
	}

	public IRI subjectIdToIRI(long id) {
		if (id <= this.hdt.getDictionary().getNshared()) {
			return valueFactory.createIRI("http://hdt.org/SO" + id);
		} else {
			return valueFactory.createIRI("http://hdt.org/S" + id);
		}
	}

	public IRI predicateIdToIRI(long id) {
		return valueFactory.createIRI("http://hdt.org/P" + id);
	}

	public IRI objectIdToIRI(long id) {
		if (id <= this.hdt.getDictionary().getNshared()) {
			return valueFactory.createIRI("http://hdt.org/SO" + id);
		} else {
			return valueFactory.createIRI("http://hdt.org/O" + id);
		}
	}

	public Resource rdf4jToHdtIDsubject(Resource subj) {
		String iriString = subj.toString();
		long id = -1;
		if (iriString.startsWith(("http://hdt.org/"))) {
			iriString = iriString.replace("http://hdt.org/", "");
			if (iriString.startsWith("SO")) {
				id = Long.parseLong(iriString.substring(2));
			} else if (iriString.startsWith("S")) {
				id = Long.parseLong(iriString.substring(1));
			}
			return IdToSubjectHDTResource(id);
		}
		return subj;
	}

	public IRI rdf4jToHdtIDpredicate(IRI pred) {
		String iriString = pred.toString();
		if (iriString.startsWith(("http://hdt.org/"))) {
			long id;
			iriString = iriString.replace("http://hdt.org/", "");
			if (iriString.startsWith("P")) {
				id = Long.parseLong(iriString.substring(1));
			} else {
				id = -1;
			}
			return IdToPredicateHDTResource(id);
		}
		return pred;
	}

	public Value rdf4jToHdtIDobject(Value object) {
		String iriString = object.toString();
		if (iriString.startsWith(("http://hdt.org/"))) {
			iriString = iriString.replace("http://hdt.org/", "");
			long id;
			if (iriString.startsWith("SO")) {
				id = Long.parseLong(iriString.substring(2));
			} else if (iriString.startsWith("O")) {
				id = Long.parseLong(iriString.substring(1));
			} else {
				id = -1;
			}
			return IdToObjectHDTResource(id);
		}
		return object;
	}

	public Resource IdToSubjectHDTResource(long subjectID) {
		if ((subjectID >= endpoint.getHdtProps().getStartBlankShared()
				&& subjectID <= endpoint.getHdtProps().getEndBlankShared())
				|| (subjectID >= endpoint.getHdtProps().getStartBlankSubjects()
						&& subjectID <= endpoint.getHdtProps().getEndBlankSubjects())) {
			if (subjectID <= hdt.getDictionary().getNshared()) {
				return new SimpleBNodeHDT(hdt, SimpleIRIHDT.SHARED_POS, subjectID);
			} else {
				return new SimpleBNodeHDT(hdt, SimpleIRIHDT.SUBJECT_POS, subjectID);
			}
		} else {
			if (subjectID <= hdt.getDictionary().getNshared()) {
				return new SimpleIRIHDT(hdt, SimpleIRIHDT.SHARED_POS, subjectID);
			} else {
				return new SimpleIRIHDT(hdt, SimpleIRIHDT.SUBJECT_POS, subjectID);
			}
		}
	}

	public IRI IdToPredicateHDTResource(long predicateId) {
		return new SimpleIRIHDT(endpoint.getHdt(), SimpleIRIHDT.PREDICATE_POS, predicateId);
	}

	public Value IdToObjectHDTResource(long objectID) {
		if (objectID >= endpoint.getHdtProps().getStartLiteral()
				&& objectID <= endpoint.getHdtProps().getEndLiteral()) {
			return new SimpleLiteralHDT(endpoint.getHdt(), objectID, valueFactory, optimizeDatatype);
		} else if ((objectID >= endpoint.getHdtProps().getStartBlankObjects()
				&& objectID <= endpoint.getHdtProps().getEndBlankObjects())
				|| (objectID >= endpoint.getHdtProps().getStartBlankShared()
						&& objectID <= endpoint.getHdtProps().getEndBlankShared())) {
			if (objectID <= hdt.getDictionary().getNshared()) {
				return new SimpleBNodeHDT(hdt, SimpleIRIHDT.SHARED_POS, objectID);
			} else {
				return new SimpleBNodeHDT(hdt, SimpleIRIHDT.OBJECT_POS, objectID);
			}
		} else {
			if (objectID <= endpoint.getHdt().getDictionary().getNshared()) {
				return new SimpleIRIHDT(endpoint.getHdt(), SimpleIRIHDT.SHARED_POS, objectID);
			} else {
				return new SimpleIRIHDT(endpoint.getHdt(), SimpleIRIHDT.OBJECT_POS, objectID);
			}
		}
	}

	public Resource subjectHdtResourceToResource(Resource subject) {
		if (subject instanceof SimpleIRIHDT) {
			return this.endpoint.getChangingStore().getValueFactory().createIRI(subject.stringValue());
		} else if (subject instanceof BNode) {
			return this.endpoint.getChangingStore().getValueFactory().createBNode(subject.stringValue());
		}
		return subject;
	}

	public IRI predicateHdtResourceToResource(IRI predicate) {
		if (predicate instanceof SimpleIRIHDT) {
			return this.endpoint.getChangingStore().getValueFactory().createIRI(predicate.stringValue());
		}
		return predicate;
	}

	public Value objectHdtResourceToResource(Value object) {
		if (object instanceof SimpleIRIHDT) {
			return this.endpoint.getChangingStore().getValueFactory().createIRI(object.stringValue());
		}
		if (object instanceof BNode) {
			return this.endpoint.getChangingStore().getValueFactory().createBNode(object.stringValue());
		}
		if (object instanceof SimpleLiteralHDT) {
			SimpleLiteralHDT literal = (SimpleLiteralHDT) object;
			if (literal.getLanguage().isPresent()) {
				return this.endpoint.getChangingStore().getValueFactory().createLiteral(literal.getLabel(),
						literal.getLanguage().get());
			} else {
				return this.endpoint.getChangingStore().getValueFactory().createLiteral(literal.getLabel(),
						literal.getDatatype());
			}
		}
		return object;
	}

	public Value idToHDTValue(long id, int position) {
		switch (position) {
		case SimpleIRIHDT.SUBJECT_POS:
		case SimpleIRIHDT.SHARED_POS:
			return IdToSubjectHDTResource(id);
		case SimpleIRIHDT.PREDICATE_POS:
			return IdToPredicateHDTResource(id);
		case SimpleIRIHDT.OBJECT_POS:
			return IdToObjectHDTResource(id);
		default:
			throw new IllegalArgumentException("bad position: " + position);
		}
	}

	public Statement rdf4ToHdt(Statement statement) {
		Resource s = rdf4jToHdtIDsubject(statement.getSubject());
		IRI p = rdf4jToHdtIDpredicate(statement.getPredicate());
		Value o = rdf4jToHdtIDobject(statement.getObject());
		if (s == statement.getSubject() && p == statement.getPredicate() && o == statement.getObject()) {
			return statement;
		}
		return valueFactory.createStatement(s, p, o);
	}

	public Statement hdtStatementToStatement(Statement statement) {
		if (!(statement.getSubject() instanceof HDTValue || statement.getPredicate() instanceof HDTValue
				|| statement.getObject() instanceof HDTValue)) {
			return statement;
		}
		Resource s = subjectHdtResourceToResource(statement.getSubject());
		IRI p = predicateHdtResourceToResource(statement.getPredicate());
		Value o = objectHdtResourceToResource(statement.getObject());
		if (s == statement.getSubject() && p == statement.getPredicate() && o == statement.getObject()) {
			return statement;
		}
		return valueFactory.createStatement(s, p, o);
	}

	public void delegate(Value obj) {
		if (obj instanceof HDTValue) {
			((HDTValue) obj).setDelegate(true);
		}
	}

	public Statement delegate(Statement statement) {
		delegate(statement.getSubject());
		delegate(statement.getPredicate());
		delegate(statement.getObject());
		return statement;
	}
}

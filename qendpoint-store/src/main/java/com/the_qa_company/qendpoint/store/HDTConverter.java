package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.RDFNodeType;
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
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtil;
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
	public static final String HDT_URI = "http://hdt.org/";
	private final EndpointStore endpoint;
	private final HDT hdt;
	private final ValueFactory valueFactory = new MemValueFactory();

	public HDTConverter(EndpointStore endpoint) {
		this.endpoint = endpoint;
		this.hdt = endpoint.getHdt();
	}

	// method to get the ID of a resource
	public long subjectToID(Resource subj) {
		if (subj == null) {
			return 0;
		}
		if (!(subj instanceof SimpleIRIHDT hdtSubj)) {
			return this.hdt.getDictionary().stringToId(subj.toString(), TripleComponentRole.SUBJECT);
		}
		// if it is a HDT IRI we do not need to make a full conversion, we
		// already have the IDs
		long id = hdtSubj.getId();
		long position = hdtSubj.getPostion();
		if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.SUBJECT_POS) {
			return id;
		}
		String translate;
		if (position == SimpleIRIHDT.PREDICATE_POS) {
			translate = hdt.getDictionary().idToString(id, TripleComponentRole.PREDICATE).toString();
		} else if (position == SimpleIRIHDT.OBJECT_POS) {
			translate = hdt.getDictionary().idToString(id, TripleComponentRole.OBJECT).toString();
		} else {
			translate = "";
		}
		id = hdt.getDictionary().stringToId(translate, TripleComponentRole.SUBJECT);
		return id;
	}

	public long predicateToID(IRI pred) {
		if (pred != null) {
			// if it is a HDT IRI we do not need to make a full conversion, we
			// already have the IDs
			if (pred instanceof SimpleIRIHDT hdtPred) {
				long id = hdtPred.getId();
				long position = hdtPred.getPostion();
				if (position == SimpleIRIHDT.PREDICATE_POS) {
					return id;
				}
				String translate;
				if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.SUBJECT_POS) {
					translate = hdt.getDictionary().idToString(id, TripleComponentRole.SUBJECT).toString();
				} else if (position == SimpleIRIHDT.OBJECT_POS) {
					translate = hdt.getDictionary().idToString(id, TripleComponentRole.OBJECT).toString();
				} else {
					translate = "";
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
			if (obj instanceof SimpleIRIHDT hdtObj) {
				long id = hdtObj.getId();
				int position = hdtObj.getPostion();
				if (position == SimpleIRIHDT.SHARED_POS || position == SimpleIRIHDT.OBJECT_POS) {
					return id;
				}
				String translate;
				if (position == SimpleIRIHDT.PREDICATE_POS) {
					translate = hdt.getDictionary().idToString(id, TripleComponentRole.PREDICATE).toString();
				} else if (position == SimpleIRIHDT.SUBJECT_POS) {
					translate = hdt.getDictionary().idToString(id, TripleComponentRole.SUBJECT).toString();
				} else {
					translate = "";
				}
				return hdt.getDictionary().stringToId(translate, TripleComponentRole.OBJECT);
			} else if (obj instanceof SimpleLiteralHDT hdtObj) {
				return hdtObj.getHdtID();
			} else {
				if (QueryEvaluationUtil.isSimpleLiteral(obj)) {
					return this.hdt.getDictionary().stringToId('"' + obj.stringValue() + '"',
							TripleComponentRole.OBJECT);
				}
				return this.hdt.getDictionary().stringToId(obj.toString(), TripleComponentRole.OBJECT);
			}
		} else {
			return 0;
		}
	}

	public IRI subjectIdToIRI(long id) {
		if (id <= this.hdt.getDictionary().getNshared()) {
			return valueFactory.createIRI(HDT_URI + "SO" + id);
		} else {
			return valueFactory.createIRI(HDT_URI + "S" + id);
		}
	}

	public IRI predicateIdToIRI(long id) {
		return valueFactory.createIRI(HDT_URI + "P" + id);
	}

	public IRI objectIdToIRI(long id) {
		if (id <= this.hdt.getDictionary().getNshared()) {
			return valueFactory.createIRI(HDT_URI + "SO" + id);
		} else {
			return valueFactory.createIRI(HDT_URI + "O" + id);
		}
	}

	public Resource rdf4jToHdtIDsubject(Resource subj) {
		long id = rdf4jSubjectToHdtID(subj);
		if (id != -1) {
			return idToSubjectHDTResource(id);
		}
		return subj;
	}

	public IRI rdf4jToHdtIDpredicate(IRI pred) {
		long id = rdf4jPredicateToHdtID(pred);
		if (id != -1) {
			return idToPredicateHDTResource(id);
		}
		return pred;
	}

	public Value rdf4jToHdtIDobject(Value object) {
		long id = rdf4jObjectToHdtID(object);
		if (id != -1) {
			return idToObjectHDTResource(id);
		}
		return object;
	}

	public long rdf4jSubjectToHdtID(Resource subj) {
		String iriString = subj.stringValue();
		if (iriString.startsWith((HDT_URI))) {
			if (iriString.startsWith("SO", HDT_URI.length())) {
				return Long.parseLong(iriString, HDT_URI.length() + 2, iriString.length(), 10);
			} else if (iriString.startsWith("S", HDT_URI.length())) {
				return Long.parseLong(iriString, HDT_URI.length() + 1, iriString.length(), 10);
			}
		}
		return -1;
	}

	public long rdf4jPredicateToHdtID(IRI pred) {
		String iriString = pred.stringValue();
		if (iriString.startsWith((HDT_URI))) {
			if (iriString.startsWith("P", HDT_URI.length())) {
				return Long.parseLong(iriString, HDT_URI.length() + 1, iriString.length(), 10);
			}
		}
		return -1;
	}

	public long rdf4jObjectToHdtID(Value object) {
		String iriString = object.stringValue();
		if (iriString.startsWith(HDT_URI)) {
			if (iriString.startsWith("SO", HDT_URI.length())) {
				return Long.parseLong(iriString, HDT_URI.length() + 2, iriString.length(), 10);
			} else if (iriString.startsWith("O", HDT_URI.length())) {
				return Long.parseLong(iriString, HDT_URI.length() + 1, iriString.length(), 10);
			}
		}
		return -1;
	}

	public Value idToValue(TripleComponentRole role, long id) {
		Dictionary dict = hdt.getDictionary();
		if (dict.supportsNodeTypeOfId()) {
			RDFNodeType nodeType = dict.nodeTypeOfId(role, id);
			boolean shared = id <= dict.getNshared();
			return switch (nodeType) {
			case IRI ->
				new SimpleIRIHDT(endpoint.getHdt(), SimpleIRIHDT.getPos(role.asDictionarySectionRole(shared)), id);
			case BLANK_NODE -> new SimpleBNodeHDT(hdt, SimpleIRIHDT.getPos(role.asDictionarySectionRole(shared)), id);
			case LITERAL -> new SimpleLiteralHDT(endpoint.getHdt(), id, valueFactory);
			};
		}
		return switch (role) {
		case SUBJECT -> idToSubjectHDTResource0(id);
		case PREDICATE -> idToPredicateHDTResource(id);
		case OBJECT -> idToObjectHDTResource0(id);
		};
	}

	public Resource idToSubjectHDTResource(long subjectID) {
		return (Resource) idToValue(TripleComponentRole.SUBJECT, subjectID);
	}

	private Resource idToSubjectHDTResource0(long subjectID) {
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

	public IRI idToPredicateHDTResource(long predicateId) {
		return new SimpleIRIHDT(endpoint.getHdt(), SimpleIRIHDT.PREDICATE_POS, predicateId);
	}

	public Value idToObjectHDTResource(long objectID) {
		return idToValue(TripleComponentRole.OBJECT, objectID);
	}

	private Value idToObjectHDTResource0(long objectID) {
		if (objectID >= endpoint.getHdtProps().getStartLiteral()
				&& objectID <= endpoint.getHdtProps().getEndLiteral()) {
			return new SimpleLiteralHDT(endpoint.getHdt(), objectID, valueFactory);
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
		if (object instanceof SimpleLiteralHDT literal) {
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
		return switch (position) {
		case SimpleIRIHDT.SUBJECT_POS, SimpleIRIHDT.SHARED_POS -> idToSubjectHDTResource(id);
		case SimpleIRIHDT.PREDICATE_POS -> idToPredicateHDTResource(id);
		case SimpleIRIHDT.OBJECT_POS -> idToObjectHDTResource(id);
		default -> throw new IllegalArgumentException("bad position: " + position);
		};
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

	public void delegate(Value obj) {
		if (obj instanceof HDTValue hdtValue) {
			hdtValue.setDelegate(true);
		}
	}

	public Statement delegate(Statement statement) {
		delegate(statement.getSubject());
		delegate(statement.getPredicate());
		delegate(statement.getObject());
		return statement;
	}

	public Value convertValue(Value value) {
		if (value == null) {
			return null;
		}
		String iriString = value.toString();
		long id = hdt.getDictionary().stringToId(iriString, TripleComponentRole.SUBJECT);
		int position;
		if (id != -1) {
			if (id <= hdt.getDictionary().getNshared()) {
				position = SimpleIRIHDT.SHARED_POS;
			} else {
				position = SimpleIRIHDT.SUBJECT_POS;
			}
		} else {
			id = hdt.getDictionary().stringToId(iriString, TripleComponentRole.OBJECT);
			if (id != -1) {
				position = SimpleIRIHDT.OBJECT_POS;
			} else {
				id = hdt.getDictionary().stringToId(iriString, TripleComponentRole.PREDICATE);
				position = SimpleIRIHDT.PREDICATE_POS;
			}
		}
		if (id != -1) {
			return idToHDTValue(id, position);
		} else {
			return null;
		}
	}
}

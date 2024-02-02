package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.RDFNodeType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
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
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

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
	private final ValueFactory valueFactory = SimpleValueFactory.getInstance();

	public HDTConverter(EndpointStore endpoint) {
		this.endpoint = endpoint;
		this.hdt = endpoint.getHdt();
	}

	// method to get the ID of a resource
	public long subjectToID(Resource subj) {
		if (subj == null) {
			return 0;
		}
		if (!(subj instanceof HDTValue hdtval)) {
			return this.hdt.getDictionary().stringToId(subj.toString(), TripleComponentRole.SUBJECT);
		}

		long id = hdtval.getHDTId();
		return switch (hdtval.getHDTPosition()) {
		case SimpleIRIHDT.SUBJECT_POS, SimpleIRIHDT.SHARED_POS -> id;
		case SimpleIRIHDT.PREDICATE_POS, SimpleIRIHDT.GRAPH_POS ->
			hdt.getDictionary().stringToId(subj.toString(), TripleComponentRole.SUBJECT);
		case SimpleIRIHDT.OBJECT_POS -> -1; // not shared
		default -> throw new IllegalArgumentException("Invalid HDT position: " + hdtval.getHDTPosition());
		};
	}

	public long predicateToID(IRI pred) {
		if (pred == null) {
			return 0;
		}

		if (!(pred instanceof HDTValue hdtval && hdtval.getHDTPosition() == SimpleIRIHDT.PREDICATE_POS)) {
			return this.hdt.getDictionary().stringToId(pred.toString(), TripleComponentRole.PREDICATE);
		}

		return hdtval.getHDTId();
	}

	public long objectToID(Value obj) {
		if (obj == null) {
			return 0;
		}
		if (!(obj instanceof HDTValue hdtval)) {
			return this.hdt.getDictionary().stringToId(obj.toString(), TripleComponentRole.OBJECT);
		}

		long id = hdtval.getHDTId();
		return switch (hdtval.getHDTPosition()) {
		case SimpleIRIHDT.OBJECT_POS, SimpleIRIHDT.SHARED_POS -> id;
		case SimpleIRIHDT.PREDICATE_POS, SimpleIRIHDT.GRAPH_POS ->
			hdt.getDictionary().stringToId(obj.toString(), TripleComponentRole.OBJECT);
		case SimpleIRIHDT.SUBJECT_POS -> -1; // not shared
		default -> throw new IllegalArgumentException("Invalid HDT position: " + hdtval.getHDTPosition());
		};
	}

	public long contextToID(Resource context) {
		if (context == null) {
			return endpoint.getHdtProps().getDefaultGraph();
		}

		if (!(context instanceof HDTValue hdtval && hdtval.getHDTPosition() == SimpleIRIHDT.GRAPH_POS)) {
			return this.hdt.getDictionary().stringToId(context.toString(), TripleComponentRole.GRAPH);
		}

		return hdtval.getHDTId();
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

	public IRI graphIdToIRI(long id) {
		return valueFactory.createIRI(HDT_URI + "G" + id);
	}

	public Resource[] graphIdToIRI(Resource[] contexts, long[] contextIds) {
		if (contexts.length == 0) {
			return contexts; // nothing to remap
		}

		Resource[] newcontexts = new Resource[contexts.length];

		for (int i = 0; i < newcontexts.length; i++) {
			contextIds[i] = contextToID(contexts[i]);
			if (contextIds[i] > 0) {
				newcontexts[i] = graphIdToIRI(contextIds[i]);
			} else {
				newcontexts[i] = contexts[i];
			}
		}

		return newcontexts;
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

	public Resource rdf4jToHdtIDcontext(Resource ctx) {
		if (ctx == null || !hdt.getDictionary().supportGraphs()) {
			return ctx;
		}
		long id = rdf4jContextToHdtID(ctx);
		if (id != -1) {
			return idToGraphHDTResource(id);
		}
		return ctx;
	}

	public long rdf4jSubjectToHdtID(Resource subj) {
		if (subj == null) {
			return -1;
		}
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
		if (pred == null) {
			return -1;
		}
		String iriString = pred.stringValue();
		if (iriString.startsWith((HDT_URI))) {
			if (iriString.startsWith("P", HDT_URI.length())) {
				return Long.parseLong(iriString, HDT_URI.length() + 1, iriString.length(), 10);
			}
		}
		return -1;
	}

	public long rdf4jObjectToHdtID(Value object) {
		if (object == null) {
			return -1;
		}
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

	public long rdf4jContextToHdtID(Resource ctx) {
		if (ctx == null) {
			return endpoint.getHdtProps().getDefaultGraph();
		}
		String iriString = ctx.stringValue();
		if (iriString.startsWith((HDT_URI))) {
			if (iriString.startsWith("G", HDT_URI.length())) {
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
		case GRAPH -> idToGraphHDTResource(id);
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

	public Resource idToGraphHDTResource(long graphID) {
		if (graphID == endpoint.getHdtProps().getDefaultGraph()) {
			return null;
		}
		if ((graphID >= endpoint.getHdtProps().getStartBlankGraph()
				&& graphID <= endpoint.getHdtProps().getEndBlankGraph())) {
			return new SimpleBNodeHDT(hdt, SimpleIRIHDT.GRAPH_POS, graphID);
		}
		return new SimpleIRIHDT(hdt, SimpleIRIHDT.GRAPH_POS, graphID);
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
		case SimpleIRIHDT.GRAPH_POS -> idToGraphHDTResource(id);
		default -> throw new IllegalArgumentException("bad position: " + position);
		};
	}

	public Statement rdf4ToHdt(Statement statement) {
		Resource s = rdf4jToHdtIDsubject(statement.getSubject());
		IRI p = rdf4jToHdtIDpredicate(statement.getPredicate());
		Value o = rdf4jToHdtIDobject(statement.getObject());
		if (hdt.getDictionary().supportGraphs()) {
			Resource g = rdf4jToHdtIDcontext(statement.getContext());
			if (s == statement.getSubject() && p == statement.getPredicate() && o == statement.getObject()
					&& g == statement.getContext()) {
				return statement;
			}
			return valueFactory.createStatement(s, p, o, g);
		}
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
		delegate(statement.getContext());
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

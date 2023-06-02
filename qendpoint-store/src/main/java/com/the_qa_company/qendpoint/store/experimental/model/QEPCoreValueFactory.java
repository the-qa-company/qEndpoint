package com.the_qa_company.qendpoint.store.experimental.model;

import com.the_qa_company.qendpoint.core.storage.QEPComponent;
import com.the_qa_company.qendpoint.core.storage.QEPCore;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.AbstractStatement;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.XSD;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.namespace.QName;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

public class QEPCoreValueFactory implements ValueFactory {
	private static final Map<QName, IRI> CALENDAR_DATATYPE;

	static {
		CALENDAR_DATATYPE = new HashMap<>();
		CALENDAR_DATATYPE.put(DatatypeConstants.DATETIME, XSD.DATETIME);
		CALENDAR_DATATYPE.put(DatatypeConstants.TIME, XSD.TIME);
		CALENDAR_DATATYPE.put(DatatypeConstants.DATE, XSD.DATE);
		CALENDAR_DATATYPE.put(DatatypeConstants.GYEARMONTH, XSD.GYEARMONTH);
		CALENDAR_DATATYPE.put(DatatypeConstants.GYEAR, XSD.GYEAR);
		CALENDAR_DATATYPE.put(DatatypeConstants.GMONTHDAY, XSD.GMONTHDAY);
		CALENDAR_DATATYPE.put(DatatypeConstants.GDAY, XSD.GDAY);
		CALENDAR_DATATYPE.put(DatatypeConstants.GMONTH, XSD.GMONTH);
		CALENDAR_DATATYPE.put(DatatypeConstants.DURATION, XSD.DURATION);
		CALENDAR_DATATYPE.put(DatatypeConstants.DURATION_DAYTIME, XSD.DAYTIMEDURATION);
		CALENDAR_DATATYPE.put(DatatypeConstants.DURATION_YEARMONTH, XSD.YEARMONTHDURATION);
	}

	private final QEPCore core;
	private final Literal TRUE;
	private final Literal FALSE;
	private final DatatypeFactory dtf;

	public QEPCoreValueFactory(QEPCore core) {
		this.core = core;

		TRUE = createLiteral("true", CoreDatatype.XSD.BOOLEAN.getIri());
		FALSE = createLiteral("false", CoreDatatype.XSD.BOOLEAN.getIri());

		try {
			dtf = DatatypeFactory.newInstance();
		} catch (DatatypeConfigurationException e) {
			throw new RuntimeException("can't get dtf", e);
		}
	}

	@Override
	public IRI createIRI(String s) {
		return new QEPCoreIRI(core.createComponentByString(s));
	}

	@Override
	public IRI createIRI(String s, String s1) {
		return null;
	}

	@Override
	public BNode createBNode() {
		return Values.bnode();
	}

	@Override
	public BNode createBNode(String s) {
		return new QEPCoreBNode(core.createComponentByString("_:" + s));
	}

	@Override
	public Literal createLiteral(String s) {
		return new QEPCoreLiteral(core.createComponentByString('"' + s + '"'));
	}

	@Override
	public Literal createLiteral(String label, String language) {
		return new QEPCoreLiteral(core.createComponentByString('"' + label + "\"@" + language));
	}

	@Override
	public Literal createLiteral(String label, IRI iri) {
		if (XSD.STRING.equals(iri)) {
			return createLiteral(label);
		}
		return new QEPCoreLiteral(core.createComponentByString('"' + label + "\"^^<" + iri.stringValue() + ">"));
	}

	@Override
	public Literal createLiteral(String s, CoreDatatype coreDatatype) {
		if (coreDatatype == CoreDatatype.NONE) {
			return createLiteral(s);
		}
		return createLiteral(s, coreDatatype.getIri());
	}

	@Override
	public Literal createLiteral(String s, IRI iri, CoreDatatype coreDatatype) {
		assert coreDatatype == CoreDatatype.NONE || coreDatatype.getIri() == iri;
		return createLiteral(s, iri);
	}

	@Override
	public Literal createLiteral(boolean b) {
		return b ? TRUE : FALSE;
	}

	@Override
	public Literal createLiteral(byte b) {
		return createLiteral(String.valueOf(b), XSD.BYTE);
	}

	@Override
	public Literal createLiteral(short i) {
		return createLiteral(String.valueOf(i), XSD.SHORT);
	}

	@Override
	public Literal createLiteral(int i) {
		return createLiteral(String.valueOf(i), XSD.INT);
	}

	@Override
	public Literal createLiteral(long l) {
		return createLiteral(String.valueOf(l), XSD.LONG);
	}

	@Override
	public Literal createLiteral(float v) {
		return createLiteral(String.valueOf(v), XSD.FLOAT);
	}

	@Override
	public Literal createLiteral(double v) {
		return createLiteral(String.valueOf(v), XSD.DOUBLE);
	}

	@Override
	public Literal createLiteral(BigDecimal bigDecimal) {
		return createLiteral(String.valueOf(bigDecimal), XSD.DECIMAL);
	}

	@Override
	public Literal createLiteral(BigInteger bigInteger) {
		return createLiteral(String.valueOf(bigInteger), XSD.LONG);
	}

	@Override
	public Literal createLiteral(XMLGregorianCalendar xmlGregorianCalendar) {
		return createLiteral(xmlGregorianCalendar.toXMLFormat(), CALENDAR_DATATYPE.get(xmlGregorianCalendar.getXMLSchemaType()));
	}

	@Override
	public Literal createLiteral(Date date) {
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		return createLiteral(dtf.newXMLGregorianCalendar(calendar));
	}

	@Override
	public QEPStatement createStatement(Resource resource, IRI iri, Value value) {
		return new QEPStatement(resource, iri, value);
	}

	@Override
	public QEPStatement createStatement(Resource resource, IRI iri, Value value, Resource ctx) {
		return new QEPStatement(resource, iri, value, ctx);
	}

	public QEPStatement asQEPStatement(Statement statement) {
		if (statement instanceof QEPStatement qs) {
			return qs;
		}
		return new QEPStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(), statement.getContext());
	}

	public final class QEPStatement extends AbstractStatement {

		private final Resource resource;
		private final IRI predicate;
		private final Value object;
		private final Resource context;

		public QEPStatement(Resource resource, IRI predicate, Value object) {
			this(resource, predicate, object, null);
		}

		public QEPStatement(Resource resource, IRI predicate, Value object, Resource context) {
			this.resource = resource;
			this.predicate = predicate;
			this.object = object;
			this.context = context;
		}

		@Override
		public Resource getSubject() {
			return resource;
		}

		@Override
		public IRI getPredicate() {
			return predicate;
		}

		@Override
		public Value getObject() {
			return object;
		}

		@Override
		public Resource getContext() {
			return context;
		}

		public QEPComponentTriple asCoreTriple() {
			final QEPComponent s;
			final QEPComponent p;
			final QEPComponent o;

			if (getSubject() instanceof QEPCoreValue qcv) {
				s = qcv.component();
			} else {
				s = core.createComponentByString(getSubject().toString());
			}

			if (getPredicate() instanceof QEPCoreValue qcv) {
				p = qcv.component();
			} else {
				p = core.createComponentByString(getPredicate().toString());
			}

			if (getObject() instanceof QEPCoreValue qcv) {
				o = qcv.component();
			} else {
				o = core.createComponentByString(getObject().toString());
			}

			return QEPComponentTriple.of(s, p, o);
		}
	}
}

package com.the_qa_company.qendpoint.store.experimental.model;

import com.the_qa_company.qendpoint.core.storage.QEPComponent;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.AbstractLiteral;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.XSD;

import java.util.Objects;
import java.util.Optional;

public class QEPCoreLiteral extends AbstractLiteral implements QEPCoreValue {
	private final QEPComponent component;
	private String label;
	private IRI datatype;
	private Optional<String> language;
	private CoreDatatype cdt;

	public QEPCoreLiteral(QEPComponent component) {
		this.component = component;
	}

	@Override
	public String getLabel() {
		if (label == null) {
			String labelVal = LiteralsUtils.removeTypeAndLang(component.getString()).toString();
			label = labelVal.substring(1, labelVal.length() - 1);
		}
		return label;
	}

	@Override
	public Optional<String> getLanguage() {
		if (language == null) {
			language = component.getLanguage().map(CharSequence::toString);
		}
		return language;
	}

	@Override
	public IRI getDatatype() {
		if (datatype != null) {
			return datatype;
		}
		CharSequence type = component.getDatatype();
		if (type == LiteralsUtils.LITERAL_LANG_TYPE) {
			datatype = CoreDatatype.RDF.LANGSTRING.getIri();
		} else if (type != LiteralsUtils.NO_DATATYPE) {
			String s = type.toString();
			datatype = Values.iri(s.substring(1, s.length() - 1));
		} else {
			datatype = XSD.STRING;
		}
		return datatype;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		if (cdt == null) {
			cdt = CoreDatatype.from(getDatatype());
		}
		return cdt;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o instanceof QEPCoreLiteral qcc) {
			return Objects.equals(component, qcc.component);
		}
		return super.equals(o);
	}

	@Override
	public int hashCode() {
		return stringValue().hashCode();
	}

	@Override
	public QEPComponent component() {
		return component;
	}
}

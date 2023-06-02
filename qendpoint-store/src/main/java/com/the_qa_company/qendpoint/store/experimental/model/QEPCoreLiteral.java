package com.the_qa_company.qendpoint.store.experimental.model;

import com.the_qa_company.qendpoint.core.storage.QEPComponent;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.AbstractLiteral;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.util.Values;

import java.util.Objects;
import java.util.Optional;

public class QEPCoreLiteral extends AbstractLiteral implements QEPCoreValue {
	private final QEPComponent component;
	private String label;
	private IRI datatype;
	private boolean findDT;
	private Optional<String> language;
	private CoreDatatype cdt;

	public QEPCoreLiteral(QEPComponent component) {
		this.component = component;
	}

	@Override
	public String getLabel() {
		if (label != null) {
			label = component.toString();
		}
		return label;
	}

	@Override
	public Optional<String> getLanguage() {
		if (language == null) {
			String label = component.toString();
			language = LiteralsUtils.getLanguage(label).map(CharSequence::toString);
		}
		return language;
	}

	@Override
	public IRI getDatatype() {
		if (findDT) {
			return datatype;
		}
		String label = component.toString();

		CharSequence type = LiteralsUtils.getType(label);
		findDT = true;
		if (type == LiteralsUtils.LITERAL_LANG_TYPE) {
			datatype = CoreDatatype.RDF.LANGSTRING.getIri();
		} else if (type != LiteralsUtils.NO_DATATYPE) {
			datatype = Values.iri(type.toString());
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
	public QEPComponent component() {
		return component;
	}
}

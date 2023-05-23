package com.the_qa_company.qendpoint.store.experimental.model;

import com.the_qa_company.qendpoint.core.storage.QEPComponent;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.URIUtil;

import java.util.Objects;

public class QEPCoreIRI implements IRI, QEPCoreValue {
	private final QEPComponent component;
	private int localNameIdx = -1;

	public QEPCoreIRI(QEPComponent component) {
		this.component = component;
	}

	@Override
	public String getNamespace() {
		String sv = toString();
		if (localNameIdx < 0) {
			localNameIdx = URIUtil.getLocalNameIndex(sv);
		}
		return sv.substring(0, localNameIdx);
	}

	@Override
	public String getLocalName() {
		String sv = toString();
		if (localNameIdx < 0) {
			localNameIdx = URIUtil.getLocalNameIndex(sv);
		}

		return sv.substring(localNameIdx);
	}

	@Override
	public String stringValue() {
		return component.toString();
	}

	@Override
	public String toString() {
		return stringValue();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof IRI bn)) {
			return false;
		}
		if (o instanceof QEPCoreIRI qcc) {
			return Objects.equals(component, qcc.component);
		}
		return Objects.equals(stringValue(), bn.stringValue());
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

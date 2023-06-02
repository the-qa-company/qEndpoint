package com.the_qa_company.qendpoint.store.experimental.model;

import com.the_qa_company.qendpoint.core.storage.QEPComponent;
import org.eclipse.rdf4j.model.BNode;

import java.util.Objects;

public class QEPCoreBNode implements BNode, QEPCoreValue {
	private final QEPComponent component;

	public QEPCoreBNode(QEPComponent component) {
		this.component = component;
	}

	@Override
	public String getID() {
		return stringValue().substring(2);
	}

	@Override
	public String stringValue() {
		return component.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof BNode bn)) {
			return false;
		}
		if (o instanceof QEPCoreBNode qcc) {
			return Objects.equals(component, qcc.component);
		}
		return Objects.equals(getID(), bn.getID());
	}

	@Override
	public int hashCode() {
		return Objects.hash(component);
	}

	@Override
	public QEPComponent component() {
		return component;
	}
}

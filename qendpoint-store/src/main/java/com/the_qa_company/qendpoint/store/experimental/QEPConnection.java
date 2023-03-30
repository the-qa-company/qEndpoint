package com.the_qa_company.qendpoint.store.experimental;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailSourceConnection;

public class QEPConnection extends SailSourceConnection {
	protected QEPConnection(ExperimentalQEndpointSail sail) {
		super(sail, sail.getSailStore(), sail.getFederatedServiceResolver());
	}

	@Override
	protected void addStatementInternal(Resource resource, IRI iri, Value value, Resource... resources) throws SailException {

	}

	@Override
	protected void removeStatementsInternal(Resource resource, IRI iri, Value value, Resource... resources) throws SailException {

	}
}

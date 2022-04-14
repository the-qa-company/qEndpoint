package com.the_qa_company.qendpoint.utils.rdf;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

public class RDFHandlerCounter implements RDFHandler {
	private final RDFHandler handler;
	private long count;

	public RDFHandlerCounter(RDFHandler handler) {
		this.handler = handler;
	}

	@Override
	public void startRDF() throws RDFHandlerException {
		handler.startRDF();
	}

	@Override
	public void endRDF() throws RDFHandlerException {
		handler.endRDF();
	}

	@Override
	public void handleNamespace(String s, String s1) throws RDFHandlerException {
		handler.handleNamespace(s, s1);
	}

	@Override
	public void handleStatement(Statement statement) throws RDFHandlerException {
		count++;
		handler.handleStatement(statement);
	}

	@Override
	public void handleComment(String s) throws RDFHandlerException {
		handler.handleComment(s);
	}

	public long getCount() {
		return count;
	}
}

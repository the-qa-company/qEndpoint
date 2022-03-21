package com.the_qa_company.q_endpoint.utils.sail.builder.compiler;

import com.the_qa_company.q_endpoint.utils.sail.builder.SailCompiler;
import com.the_qa_company.q_endpoint.utils.sail.linked.LinkedSail;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.sail.NotifyingSail;

public abstract class LinkedSailCompiler {
	private final IRI iri;

	public LinkedSailCompiler(IRI iri) {
		this.iri = iri;
	}

	/**
	 * compile a sail from param
	 * @param reader the reader
	 * @param rnode the node describing the sail
	 * @return the sail
	 * @throws SailCompiler.SailCompilerException compiler error
	 */
	public abstract LinkedSail<? extends NotifyingSail> compileWithParam(SailCompiler.SailCompilerReader reader, Resource rnode) throws SailCompiler.SailCompilerException;

	public IRI getIri() {
		return iri;
	}
}

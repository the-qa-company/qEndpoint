package com.the_qa_company.qendpoint.utils.sail.builder.compiler;

import com.the_qa_company.qendpoint.utils.sail.builder.SailCompiler;
import com.the_qa_company.qendpoint.utils.sail.linked.LinkedSail;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.sail.NotifyingSail;

/**
 * A compiler to compile a node, register it with
 * {@link com.the_qa_company.qendpoint.utils.sail.builder.SailCompiler#registerCustomCompiler(LinkedSailCompiler)}
 * @author Antoine Willerval
 */
public abstract class LinkedSailCompiler {
	private final IRI iri;

	/**
	 * create the compiler
	 * @param iri the IRI to describe the node Compiler type
	 */
	protected LinkedSailCompiler(IRI iri) {
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

	/**
	 * @return the IRI describing the node compiler type
	 */
	public IRI getIri() {
		return iri;
	}
}

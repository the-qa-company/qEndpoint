package com.the_qa_company.qendpoint.compiler.sail;

import com.the_qa_company.qendpoint.compiler.SailCompiler;
import com.the_qa_company.qendpoint.utils.sail.linked.LinkedSail;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.sail.NotifyingSail;

/**
 * A sail to compile a node, register it with
 * {@link com.the_qa_company.qendpoint.compiler.SailCompiler#registerCustomCompiler(LinkedSailCompiler)}
 *
 * @author Antoine Willerval
 */
public abstract class LinkedSailCompiler {
	private final IRI iri;

	/**
	 * create the sail
	 *
	 * @param iri the IRI to describe the node Compiler type
	 */
	protected LinkedSailCompiler(IRI iri) {
		this.iri = iri;
	}

	/**
	 * compile a sail from param
	 *
	 * @param reader the reader
	 * @param rnode  the node describing the sail
	 * @return the sail
	 * @throws SailCompiler.SailCompilerException sail error
	 */
	public abstract LinkedSail<? extends NotifyingSail> compileWithParam(SailCompiler.SailCompilerReader reader,
			Resource rnode) throws SailCompiler.SailCompilerException;

	/**
	 * @return the IRI describing the node sail type
	 */
	public IRI getIri() {
		return iri;
	}
}

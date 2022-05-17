package com.the_qa_company.qendpoint.compiler;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

/**
 * describe a closeable triple getter
 *
 * @author Antoine Willerval
 */
public interface TripleSourceGetter extends AutoCloseable {
    @Override
    void close() throws SailCompiler.SailCompilerException;

    /**
     * get a triple
     *
     * @param s
     *            subject (or null for wildcard)
     * @param p
     *            predicate (or null for wildcard)
     * @param o
     *            object (or null for wildcard)
     *
     * @return triples iterator
     *
     * @throws SailCompiler.SailCompilerException
     *             error while getting the triples
     */
    CloseableIteration<Statement, SailCompiler.SailCompilerException> getStatements(Resource s, IRI p, Value o)
            throws SailCompiler.SailCompilerException;
}

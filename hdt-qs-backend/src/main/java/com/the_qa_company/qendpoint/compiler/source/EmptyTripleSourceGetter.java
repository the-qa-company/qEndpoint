package com.the_qa_company.qendpoint.compiler.source;

import com.the_qa_company.qendpoint.compiler.SailCompiler;
import com.the_qa_company.qendpoint.compiler.TripleSourceGetter;
import com.the_qa_company.qendpoint.compiler.TripleSourceModel;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

/**
 * empty implementation of {@link com.the_qa_company.qendpoint.compiler.TripleSourceModel}
 *
 * @author Antoine Willerval
 */
public class EmptyTripleSourceGetter implements TripleSourceGetter, TripleSourceModel {
    @Override
    public void close() throws SailCompiler.SailCompilerException {
        // nothing
    }

    @Override
    public CloseableIteration<Statement, SailCompiler.SailCompilerException> getStatements(Resource s, IRI p, Value o)
            throws SailCompiler.SailCompilerException {
        return new CloseableIteration<>() {
            @Override
            public void close() throws SailCompiler.SailCompilerException {
            }

            @Override
            public boolean hasNext() throws SailCompiler.SailCompilerException {
                return false;
            }

            @Override
            public Statement next() throws SailCompiler.SailCompilerException {
                return null;
            }

            @Override
            public void remove() throws SailCompiler.SailCompilerException {
                throw new IllegalArgumentException("empty iteration");
            }
        };
    }

    @Override
    public TripleSourceGetter getGetter() {
        return this;
    }
}

package com.the_qa_company.qendpoint.compiler.source;

import com.the_qa_company.qendpoint.compiler.SailCompiler;
import com.the_qa_company.qendpoint.compiler.TripleSourceGetter;
import com.the_qa_company.qendpoint.compiler.TripleSourceModel;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

import java.util.Iterator;

/**
 * implementation of
 * {@link com.the_qa_company.qendpoint.compiler.TripleSourceModel} to query
 * {@link org.eclipse.rdf4j.model.Model}
 *
 * @author Antoine Willerval
 */
public class ModelTripleSourceGetter implements TripleSourceGetter, TripleSourceModel {
	private final Model model;

	public ModelTripleSourceGetter(Model model) {
		this.model = model;
	}

	@Override
	public void close() throws SailCompiler.SailCompilerException {
		// nothing needed
	}

	@Override
	public CloseableIteration<Statement, SailCompiler.SailCompilerException> getStatements(Resource s, IRI p, Value o) {
		final Iterator<Statement> it = model.getStatements(s, p, o).iterator();
		return new CloseableIteration<>() {
			@Override
			public void close() {
				// nothing needed
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public Statement next() {
				return it.next();
			}

			@Override
			public void remove() {
				it.remove();
			}
		};
	}

	@Override
	public TripleSourceGetter getGetter() {
		return this;
	}
}

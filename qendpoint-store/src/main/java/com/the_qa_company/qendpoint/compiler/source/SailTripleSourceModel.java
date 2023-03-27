package com.the_qa_company.qendpoint.compiler.source;

import com.the_qa_company.qendpoint.compiler.SailCompiler;
import com.the_qa_company.qendpoint.compiler.TripleSourceGetter;
import com.the_qa_company.qendpoint.compiler.TripleSourceModel;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;

/**
 * implementation of
 * {@link com.the_qa_company.qendpoint.compiler.TripleSourceModel} to query
 * {@link org.eclipse.rdf4j.sail.Sail}
 *
 * @author Antoine Willerval
 */
public class SailTripleSourceModel implements TripleSourceModel {
	private final Sail sail;

	public SailTripleSourceModel(Sail sail) {
		this.sail = sail;
	}

	@Override
	public TripleSourceGetter getGetter() {
		return new SailTripleSourceGetter();
	}

	private class SailTripleSourceGetter implements TripleSourceGetter {

		private final SailConnection connection;

		SailTripleSourceGetter() throws SailCompiler.SailCompilerException {
			connection = sail.getConnection();
			try {
				connection.begin();
			} catch (SailException e) {
				throw new SailCompiler.SailCompilerException(e);
			}
		}

		@Override
		public void close() throws SailCompiler.SailCompilerException {
			try {
				try {
					connection.commit();
				} finally {
					connection.close();
				}
			} catch (SailException e) {
				throw new SailCompiler.SailCompilerException(e);
			}
		}

		@Override
		public CloseableIteration<Statement, SailCompiler.SailCompilerException> getStatements(Resource s, IRI p,
				Value o) {
			CloseableIteration<? extends Statement, SailException> it = sail.getConnection().getStatements(s, p, o,
					false);
			return new CloseableIteration<>() {
				@Override
				public void close() throws SailCompiler.SailCompilerException {
					try {
						it.close();
					} catch (SailException e) {
						throw new SailCompiler.SailCompilerException(e);
					}
				}

				@Override
				public boolean hasNext() throws SailCompiler.SailCompilerException {
					try {
						return it.hasNext();
					} catch (SailException e) {
						throw new SailCompiler.SailCompilerException(e);
					}
				}

				@Override
				public Statement next() throws SailCompiler.SailCompilerException {
					try {
						return it.next();
					} catch (SailException e) {
						throw new SailCompiler.SailCompilerException(e);
					}
				}

				@Override
				public void remove() throws SailCompiler.SailCompilerException {
					try {
						it.remove();
					} catch (SailException e) {
						throw new SailCompiler.SailCompilerException(e);
					}
				}
			};
		}
	}
}

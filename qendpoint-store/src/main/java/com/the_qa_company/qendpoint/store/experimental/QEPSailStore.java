package com.the_qa_company.qendpoint.store.experimental;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.storage.QEPCore;
import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.Changeset;
import org.eclipse.rdf4j.sail.base.SailDataset;
import org.eclipse.rdf4j.sail.base.SailSink;
import org.eclipse.rdf4j.sail.base.SailSource;
import org.eclipse.rdf4j.sail.base.SailStore;

import java.util.Set;

public class QEPSailStore implements SailStore {
	private final ExperimentalQEndpointSail sail;

	// config
	private final QEPCore core;

	public QEPSailStore(ExperimentalQEndpointSail sail, HDTOptions options) throws QEPCoreException {
		this.sail = sail;

		core = new QEPCore(sail.getStoreLocation(), options);
	}

	@Override
	public ValueFactory getValueFactory() {
		return null;
	}

	@Override
	public EvaluationStatistics getEvaluationStatistics() {
		return null;
	}

	@Override
	public SailSource getExplicitSailSource() {
		return new QEPSailSource(true);
	}

	@Override
	public SailSource getInferredSailSource() {
		return new QEPSailSource(false);
	}

	@Override
	public void close() throws SailException {
		try {
			core.close();
		} catch (QEPCoreException e) {
			throw new SailException(e);
		}
	}

	public class QEPSailSource implements SailSource {
		private final boolean explicit;

		public QEPSailSource(boolean explicit) {
			this.explicit = explicit;
		}

		@Override
		public SailSource fork() {
			throw new UnsupportedOperationException("This store does not support multiple datasets");
		}

		@Override
		public SailSink sink(IsolationLevel level) throws SailException {
			return new QEPSailSink(explicit);
		}

		@Override
		public SailDataset dataset(IsolationLevel level) throws SailException {
			return new QEPSailDataset(explicit);
		}

		@Override
		public void prepare() throws SailException {

		}

		@Override
		public void flush() throws SailException {

		}

		@Override
		public void close() throws SailException {

		}
	}

	public class QEPSailSink implements SailSink {
		private final boolean explicit;

		public QEPSailSink(boolean explicit) {
			this.explicit = explicit;
		}

		@Override
		public void prepare() throws SailException {
			// serializable is not supported at this level
		}

		@Override
		public void flush() throws SailException {

		}

		@Override
		public void setNamespace(String prefix, String name) throws SailException {

		}

		@Override
		public void removeNamespace(String prefix) throws SailException {

		}

		@Override
		public void clearNamespaces() throws SailException {

		}

		@Override
		public void clear(Resource... contexts) throws SailException {

		}

		@Override
		public void observe(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
			// serializable is not supported at this level
		}

		@Override
		public void observeAll(Set<Changeset.SimpleStatementPattern> observed) {
			// serializable is not supported at this level
		}

		@Override
		public void approve(Resource subj, IRI pred, Value obj, Resource ctx) throws SailException {

		}

		@Override
		public void deprecate(Statement statement) throws SailException {

		}

		@Override
		public void close() throws SailException {
			// no-op
		}
	}

	public class QEPSailDataset implements SailDataset {
		private final boolean explicit;

		public QEPSailDataset(boolean explicit) {
			this.explicit = explicit;
		}

		@Override
		public void close() throws SailException {
			// no-op
		}

		@Override
		public CloseableIteration<? extends Namespace, SailException> getNamespaces() throws SailException {
			return null;
		}

		@Override
		public String getNamespace(String prefix) throws SailException {
			return null;
		}

		@Override
		public CloseableIteration<? extends Resource, SailException> getContextIDs() throws SailException {
			return null;
		}

		@Override
		public CloseableIteration<? extends Statement, SailException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
			return null;
		}
	}
}

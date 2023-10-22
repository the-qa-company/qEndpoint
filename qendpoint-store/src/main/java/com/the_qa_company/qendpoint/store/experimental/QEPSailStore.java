package com.the_qa_company.qendpoint.store.experimental;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.storage.QEPComponent;
import com.the_qa_company.qendpoint.core.storage.QEPCore;
import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import com.the_qa_company.qendpoint.core.storage.QEPImporter;
import com.the_qa_company.qendpoint.core.storage.iterator.CloseableIterator;
import com.the_qa_company.qendpoint.store.experimental.model.QEPCloseableIteration;
import com.the_qa_company.qendpoint.store.experimental.model.QEPCoreValueFactory;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.BackingSailSource;
import org.eclipse.rdf4j.sail.base.Changeset;
import org.eclipse.rdf4j.sail.base.SailDataset;
import org.eclipse.rdf4j.sail.base.SailSink;
import org.eclipse.rdf4j.sail.base.SailSource;
import org.eclipse.rdf4j.sail.base.SailStore;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class QEPSailStore implements SailStore {
	private final ExperimentalQEndpointSail sail;

	// config
	private final QEPCore core;
	private final QEPCoreValueFactory vf;
	private final QEPCoreEvaluationStatistics statistics;
	private final ReentrantLock sinkStoreAccessLock = new ReentrantLock();
	private final AtomicBoolean storeTxnStarted = new AtomicBoolean(false);

	public QEPSailStore(ExperimentalQEndpointSail sail, HDTOptions options) throws QEPCoreException {
		this.sail = sail;

		core = new QEPCore(sail.getStoreLocation(), options);
		vf = new QEPCoreValueFactory(core);
		statistics = new QEPCoreEvaluationStatistics(this);
	}

	public QEPCore getCore() {
		return core;
	}

	@Override
	public QEPCoreValueFactory getValueFactory() {
		return vf;
	}

	@Override
	public EvaluationStatistics getEvaluationStatistics() {
		return statistics;
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

	public class QEPSailSource extends BackingSailSource {
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
	}

	public class QEPSailSink implements SailSink {
		private final boolean explicit;
		private final QEPImporter importer;

		public QEPSailSink(boolean explicit) {
			this.explicit = explicit;
			this.importer = core.createImporter();
		}

		@Override
		public void prepare() throws SailException {
			// serializable is not supported at this level
		}

		@Override
		public void flush() throws SailException {
			sinkStoreAccessLock.lock();
			try {
				try {
					try {
						core.getNamespaceData().sync();
					} finally {
						importer.endTransaction(true);
					}
				} catch (QEPCoreException qepc) {
					throw new SailException(qepc);
				}
			} finally {
				sinkStoreAccessLock.unlock();
			}
		}

		@Override
		public void setNamespace(String prefix, String name) throws SailException {
			core.getNamespaceData().setNamespace(prefix, name);
		}

		@Override
		public void removeNamespace(String prefix) throws SailException {
			core.getNamespaceData().removeNamespace(prefix);
		}

		@Override
		public void clearNamespaces() throws SailException {
			core.getNamespaceData().clear();
		}

		@Override
		public void clear(Resource... contexts) throws SailException {
			if (!explicit) {
				return;
			}
			if (contexts.length == 0) {
				deprecateByQuery(null, null, null);
			}
			for (Resource ctx : contexts) {
				if (ctx == null) {
					deprecateByQuery(null, null, null);
					continue;
				}
				// context aren't supported
			}
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
			importer.startTransaction();
			importer.insertTriple(vf.createStatement(subj, pred, obj).asCoreTriple().tripleString());
		}

		@Override
		public void deprecate(Statement statement) throws SailException {
			deprecateByQuery(statement.getSubject(), statement.getPredicate(), statement.getObject(),
					statement.getContext());
		}

		@Override
		public boolean deprecateByQuery(Resource subj, IRI pred, Value obj, Resource... contexts) {
			QEPComponent s = vf.asQEPComponent(subj);
			QEPComponent p = vf.asQEPComponent(pred);
			QEPComponent o = vf.asQEPComponent(obj);

			return core.removeTriple(s, p, o) > 0;
		}

		@Override
		public boolean supportsDeprecateByQuery() {
			return true;
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
		public QEPCloseableIteration<? extends Namespace> getNamespaces() throws SailException {
			Map<String, String> namespaces = core.getNamespaceData().getNamespaces();
			return QEPCloseableIteration.of(CloseableIterator.of(
					namespaces.entrySet().stream().map(e -> new SimpleNamespace(e.getKey(), e.getValue())).iterator()));
		}

		@Override
		public String getNamespace(String prefix) throws SailException {
			return core.getNamespaceData().getNamespace(prefix);
		}

		@Override
		public QEPCloseableIteration<? extends Resource> getContextIDs() throws SailException {
			return QEPCloseableIteration.of();
		}

		@Override
		public QEPCloseableIteration<? extends Statement> getStatements(Resource subj, IRI pred,
				Value obj, Resource... contexts) throws SailException {
			if (!explicit) {
				return QEPCloseableIteration.of();
			}

			return QEPCloseableIteration
					.of(core.search(vf.asQEPComponent(subj), vf.asQEPComponent(pred), vf.asQEPComponent(obj)))
					.map(vf::fromQEPStatement, t -> {
						if (t instanceof QEPCoreException) {
							return new SailException(t);
						}
						return null;
					});
		}
	}
}

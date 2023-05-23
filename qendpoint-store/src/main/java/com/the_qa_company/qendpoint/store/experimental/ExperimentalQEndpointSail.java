package com.the_qa_company.qendpoint.store.experimental;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.storage.QEPCore;
import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import org.apache.commons.io.file.PathUtils;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailStore;
import org.eclipse.rdf4j.sail.base.SnapshotSailStore;
import org.eclipse.rdf4j.sail.helpers.AbstractNotifyingSail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * QEndpoint sail
 *
 * @author Antoine Willerval
 */
public class ExperimentalQEndpointSail extends AbstractNotifyingSail {

	private static final Logger logger = LoggerFactory.getLogger(ExperimentalQEndpointSail.class);
	private Path storeLocation;
	private boolean isTempLocation;
	private SailStore sailStore;
	private QEPSailStore qepSailStore;
	private FederatedServiceResolver serviceResolver;
	private SPARQLServiceResolver dependentServiceResolver;
	private final HDTOptions options;

	/**
	 * Create QEP sail
	 */
	public ExperimentalQEndpointSail() {
		this(null, HDTOptions.empty());
	}

	/**
	 * Create QEP sail
	 *
	 * @param location location of the store
	 */
	public ExperimentalQEndpointSail(Path location) {
		this(location, HDTOptions.empty());
	}

	/**
	 * Create QEP sail
	 *
	 * @param options options of the store
	 */
	public ExperimentalQEndpointSail(HDTOptions options) {
		this(null, options);
	}

	/**
	 * Create QEP sail
	 *
	 * @param location location of the store
	 * @param options  options of the store
	 */
	public ExperimentalQEndpointSail(Path location, HDTOptions options) {
		setStoreLocation(location);
		this.options = options;
	}

	private void checkInit() {
		if (sailStore == null) {
			throw new SailException("The sail isn't initialized!");
		}
	}

	/**
	 * @return the store location
	 */
	public synchronized Path getStoreLocation() {
		return storeLocation;
	}

	/**
	 * set the store location
	 *
	 * @param storeLocation store location
	 * @throws SailException if the store is already init
	 */
	public synchronized void setStoreLocation(Path storeLocation) {
		if (sailStore != null) {
			throw new SailException("The store is already initialize!");
		}
		this.storeLocation = storeLocation;
	}

	@Override
	protected synchronized void shutDownInternal() throws SailException {
		try {
			try {
				if (sailStore != null) {
					sailStore.close();
				}
			} finally {
				sailStore = null;
				qepSailStore = null;
				try {
					if (dependentServiceResolver != null) {
						dependentServiceResolver.shutDown();
					}
				} finally {
					dependentServiceResolver = null;
				}
			}
		} catch (Throwable t) {
			if (isTempLocation) {
				try {
					PathUtils.deleteDirectory(storeLocation);
				} catch (IOException e) {
					t.addSuppressed(new SailException(e));
				} finally {
					isTempLocation = false;
				}
			}
		}
		if (isTempLocation) {
			try {
				PathUtils.deleteDirectory(storeLocation);
			} catch (IOException e) {
				throw new SailException(e);
			} finally {
				isTempLocation = false;
			}
		}
	}

	@Override
	protected NotifyingSailConnection getConnectionInternal() throws SailException {
		checkInit();
		return new QEPConnection(this);
	}

	public SailStore getSailStore() {
		return sailStore;
	}

	public QEPCore getQepCore() {
		return qepSailStore.getCore();
	}

	@Override
	public boolean isWritable() throws SailException {
		return true;
	}

	@Override
	protected void initializeInternal() throws SailException {
		if (sailStore != null) {
			// already init
			return;
		}

		if (storeLocation == null) {
			// create a temp directory for our store
			logger.warn("Store location unset, using temporary folder, all data will be lost!");
			try {
				storeLocation = Files.createTempDirectory("qep");
			} catch (IOException e) {
				throw new SailException("Can't create temp directory!", e);
			}
			isTempLocation = true;
		} else {
			// re init case
			isTempLocation = false;
		}

		try {
			qepSailStore = new QEPSailStore(this, options);
			sailStore = new SnapshotSailStore(qepSailStore, TreeModel::new);
		} catch (QEPCoreException e) {
			throw new SailException(e);
		}
	}

	@Override
	public ValueFactory getValueFactory() {
		return sailStore.getValueFactory();
	}

	/**
	 * @return Returns the SERVICE resolver.
	 */
	public synchronized FederatedServiceResolver getFederatedServiceResolver() {
		checkInit();
		if (serviceResolver == null) {
			if (dependentServiceResolver == null) {
				dependentServiceResolver = new SPARQLServiceResolver();
			}
			setFederatedServiceResolver(dependentServiceResolver);
		}
		return serviceResolver;
	}

	public synchronized void setFederatedServiceResolver(FederatedServiceResolver serviceResolver) {
		this.serviceResolver = serviceResolver;
	}
}

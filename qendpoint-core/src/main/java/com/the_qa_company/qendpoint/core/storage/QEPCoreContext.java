package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.storage.iterator.AutoCloseableGeneric;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Core context object, when a context is created, all the delete bitmaps and
 * datasets are set and can't be replaced, leading to a static context. It
 * should be closed. A core context is created using
 * {@link QEPCore#createSearchContext()}.
 *
 * @author Antoine Willerval
 */
public class QEPCoreContext implements AutoCloseableGeneric<QEPCoreException> {
	private final Map<Integer, QEPDatasetContext> contexts;
	private final QEPCore core;

	QEPCoreContext(QEPCore core, List<QEPDataset> contexts) {
		this.core = core;
		this.contexts = contexts.stream().collect(Collectors.toMap(QEPDataset::uid, QEPDataset::createContext));
	}

	/**
	 * get the dataset context with a dataset UID
	 *
	 * @param uid dataset uid
	 * @return the dataset context
	 */
	public QEPDatasetContext getContextForDataset(int uid) {
		return contexts.get(uid);
	}

	/**
	 * @return the core
	 */
	public QEPCore getCore() {
		return core;
	}

	/**
	 * @return the contexts created with this context
	 */
	public Collection<QEPDatasetContext> getContexts() {
		return contexts.values();
	}

	@Override
	public void close() throws QEPCoreException {
		AutoCloseableGeneric.closeAll(contexts.values());
	}
}

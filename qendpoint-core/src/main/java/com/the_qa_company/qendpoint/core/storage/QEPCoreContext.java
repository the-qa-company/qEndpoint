package com.the_qa_company.qendpoint.core.storage;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QEPCoreContext {
	private final Map<Integer, QEPDatasetContext> contexts;
	private final QEPCore core;

	QEPCoreContext(QEPCore core, List<QEPDataset> contexts) {
		this.core = core;
		this.contexts = contexts.stream().collect(Collectors.toMap(QEPDataset::uid, QEPDataset::createContext));
	}

	public QEPDatasetContext getContextForDataset(int uid) {
		return contexts.get(uid);
	}

	public QEPCore getCore() {
		return core;
	}

	public Collection<QEPDatasetContext> getContexts() {
		return contexts.values();
	}
}

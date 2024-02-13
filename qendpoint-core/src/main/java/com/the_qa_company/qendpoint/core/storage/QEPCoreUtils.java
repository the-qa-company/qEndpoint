package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.util.debug.DebugInjectionPointManager;

/**
 * Utilities for QEPCore
 */
public class QEPCoreUtils {
	public static boolean isComponentStringGenerated(QEPComponent component) {
		return component.value != null;
	}

	public static boolean isDatatypeStringGenerated(QEPComponent component) {
		return component.datatype != null;
	}

	public static boolean isLanguageStringGenerated(QEPComponent component) {
		return component.language != null;
	}

	public static boolean isRdfNodeTypeGenerated(QEPComponent component) {
		return component.rdfNodeType != null;
	}

	/**
	 * @return debug
	 */
	public static DebugInjectionPointManager.DebugInjectionPoint<QEPCore> getDebugPreBindInsert() {
		return QEPCore.preBindInsert;
	}

	/**
	 * @return debug
	 */
	public static DebugInjectionPointManager.DebugInjectionPoint<QEPCore> getDebugPostBindInsert() {
		return QEPCore.postBindInsert;
	}

	/**
	 * @return debug
	 */
	public static DebugInjectionPointManager.DebugInjectionPoint<QEPMap> getDebugPreSync() {
		return QEPMap.preSync;
	}

	/**
	 * @return debug
	 */
	public static DebugInjectionPointManager.DebugInjectionPoint<QEPMap> getDebugPostSync() {
		return QEPMap.endSync;
	}
}

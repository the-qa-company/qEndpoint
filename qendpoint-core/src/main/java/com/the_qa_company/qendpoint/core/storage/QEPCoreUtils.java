package com.the_qa_company.qendpoint.core.storage;

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
}

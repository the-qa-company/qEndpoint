package com.the_qa_company.qendpoint.core.storage;

/**
 * Exception linked with the {@link QEPCore}
 *
 * @author Antoine Willerval
 */
public class QEPCoreException extends Exception {
	public QEPCoreException() {
	}

	public QEPCoreException(String message) {
		super(message);
	}

	public QEPCoreException(String message, Throwable cause) {
		super(message, cause);
	}

	public QEPCoreException(Throwable cause) {
		super(cause);
	}

	public QEPCoreException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

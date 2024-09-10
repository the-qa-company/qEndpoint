package com.the_qa_company.qendpoint.core.storage;

import java.io.IOException;

public class QEPMapSyncIOException extends IOException {
	public QEPMapSyncIOException() {
	}

	public QEPMapSyncIOException(String message) {
		super(message);
	}

	public QEPMapSyncIOException(String message, Throwable cause) {
		super(message, cause);
	}

	public QEPMapSyncIOException(Throwable cause) {
		super(cause);
	}
}

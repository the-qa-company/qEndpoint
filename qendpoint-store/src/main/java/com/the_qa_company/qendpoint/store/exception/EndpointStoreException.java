package com.the_qa_company.qendpoint.store.exception;

public class EndpointStoreException extends RuntimeException {
	public EndpointStoreException() {
	}

	public EndpointStoreException(String message) {
		super(message);
	}

	public EndpointStoreException(String message, Throwable cause) {
		super(message, cause);
	}

	public EndpointStoreException(Throwable cause) {
		super(cause);
	}

	public EndpointStoreException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

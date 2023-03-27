package com.the_qa_company.qendpoint.store.exception;

public class EndpointStoreInputException extends EndpointStoreException {
	public EndpointStoreInputException() {
	}

	public EndpointStoreInputException(String message) {
		super(message);
	}

	public EndpointStoreInputException(String message, Throwable cause) {
		super(message, cause);
	}

	public EndpointStoreInputException(Throwable cause) {
		super(cause);
	}

	public EndpointStoreInputException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

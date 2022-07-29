package com.the_qa_company.qendpoint.store.exception;

public class EndpointTimeoutException extends EndpointStoreException {
	public EndpointTimeoutException() {
		super("Timeout");
	}
}

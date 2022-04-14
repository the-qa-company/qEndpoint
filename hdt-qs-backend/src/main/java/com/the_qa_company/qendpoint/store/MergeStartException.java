package com.the_qa_company.qendpoint.store;

public class MergeStartException extends RuntimeException {
	public MergeStartException(String message) {
		super(message);
	}

	public MergeStartException(String message, Throwable cause) {
		super(message, cause);
	}

	public MergeStartException(Throwable cause) {
		super(cause);
	}
}

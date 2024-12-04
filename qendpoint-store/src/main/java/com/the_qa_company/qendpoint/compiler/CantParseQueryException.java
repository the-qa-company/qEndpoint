package com.the_qa_company.qendpoint.compiler;

public class CantParseQueryException extends RuntimeException {

	public CantParseQueryException(String message) {
		super(message);
	}

	public CantParseQueryException(String message, Throwable cause) {
		super(message, cause);
	}

	public CantParseQueryException(Throwable cause) {
		super(cause);
	}
}

package com.the_qa_company.qendpoint.core.exceptions;

import java.io.IOException;

public class SignatureIOException extends IOException {
	public SignatureIOException() {
	}

	public SignatureIOException(String message) {
		super(message);
	}

	public SignatureIOException(String message, Throwable cause) {
		super(message, cause);
	}

	public SignatureIOException(Throwable cause) {
		super(cause);
	}
}

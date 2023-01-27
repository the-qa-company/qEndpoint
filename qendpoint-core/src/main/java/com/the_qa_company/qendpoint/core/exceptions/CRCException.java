package com.the_qa_company.qendpoint.core.exceptions;

import java.io.IOException;

public class CRCException extends IOException {

	private static final long serialVersionUID = -1663727945336553315L;

	public CRCException() {
		super();
	}

	public CRCException(String reason) {
		super(reason);
	}
}

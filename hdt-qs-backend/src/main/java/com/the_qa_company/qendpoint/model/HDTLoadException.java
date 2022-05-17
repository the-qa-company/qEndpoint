package com.the_qa_company.qendpoint.model;

public class HDTLoadException extends RuntimeException {
    public HDTLoadException() {
    }

    public HDTLoadException(String message) {
        super(message);
    }

    public HDTLoadException(String message, Throwable cause) {
        super(message, cause);
    }

    public HDTLoadException(Throwable cause) {
        super(cause);
    }
}

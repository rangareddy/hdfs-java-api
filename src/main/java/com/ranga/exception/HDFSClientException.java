package com.ranga.exception;

public class HDFSClientException extends RuntimeException {

    public HDFSClientException(String message) {
        super(message);
    }

    public HDFSClientException(Throwable throwable) {
        super(throwable);
    }

    public HDFSClientException(String message, Throwable throwable) {
        super(message, throwable);
    }

}

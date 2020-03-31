package com.ranga.exception;

public class HDFSFileException extends RuntimeException {

    public HDFSFileException(String message) {
        super(message);
    }

    public HDFSFileException(Throwable throwable) {
        super(throwable);
    }

    public HDFSFileException(String message, Throwable throwable) {
        super(message, throwable);
    }

}

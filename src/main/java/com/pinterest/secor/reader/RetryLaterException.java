package com.pinterest.secor.reader;

public class RetryLaterException extends Exception {
    public RetryLaterException(String message) {
        super(message);
    }
}

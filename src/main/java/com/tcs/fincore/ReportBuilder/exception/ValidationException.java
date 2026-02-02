package com.tcs.fincore.ReportBuilder.exception;

public class ValidationException extends RuntimeException {
    public ValidationException(String msg) { super(msg); }
    public ValidationException(String msg, Throwable t) { super(msg, t); }
}
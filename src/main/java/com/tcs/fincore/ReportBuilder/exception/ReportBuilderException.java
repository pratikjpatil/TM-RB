package com.tcs.fincore.ReportBuilder.exception;

/**
 * Base exception for all ReportBuilder domain errors.
 * Separates recoverable (ValidationException) from unrecoverable errors.
 */
public class ReportBuilderException extends RuntimeException {
    public ReportBuilderException(String message) { super(message); }
    public ReportBuilderException(String message, Throwable cause) { super(message, cause); }
}
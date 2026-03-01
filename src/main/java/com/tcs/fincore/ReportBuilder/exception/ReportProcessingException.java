package com.tcs.fincore.ReportBuilder.exception;

/**
 * Unrecoverable processing failure — the report cannot be generated.
 */
public class ReportProcessingException extends ReportBuilderException {
    public ReportProcessingException(String message, Throwable cause) { super(message, cause); }
}
package com.tcs.fincore.ReportBuilder.exception;

/**
 * Infrastructure failure — DB down, HDFS unavailable, etc.
 */
public class InfrastructureException extends ReportBuilderException {
    public InfrastructureException(String message, Throwable cause) { super(message, cause); }
}
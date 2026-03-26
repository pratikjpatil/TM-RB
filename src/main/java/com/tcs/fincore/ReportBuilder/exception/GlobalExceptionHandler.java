package com.tcs.fincore.ReportBuilder.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * SECURITY: Global exception handler ensures no internal exception details
 * are ever returned in HTTP responses. All errors are logged internally
 * with a correlationId that clients can reference when contacting support.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(ValidationException ex) {
        // Validation errors are safe to surface — they are our own controlled messages
        log.warn("Validation error: correlationId={} jobId={} templateId={} message={}", UUID.randomUUID(), "N/A", "N/A", ex.getMessage());
        return ResponseEntity.badRequest().body(errorBody(ex.getMessage(), null));
    }

    @ExceptionHandler({ReportProcessingException.class, InfrastructureException.class})
    public ResponseEntity<Map<String, Object>> handleDomainFailures(RuntimeException ex) {
        String correlationId = UUID.randomUUID().toString();
        log.error("Unhandled domain failure: correlationId={} jobId={} templateId={} errorType={}",
                correlationId, "N/A", "N/A", ex.getClass().getSimpleName(), ex);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(errorBody("Internal report processing failure. Contact support.", correlationId));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleAll(Exception ex) {
        String correlationId = UUID.randomUUID().toString();
        // SECURITY: Full exception (including cause chain) logged internally only
        log.error("Unhandled exception: correlationId={} jobId={} templateId={} errorType={}", correlationId, "N/A", "N/A", ex.getClass().getName(), ex);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(errorBody("An unexpected error occurred. Contact support.", correlationId));
    }

    private Map<String, Object> errorBody(String message, String correlationId) {
        var body = new java.util.LinkedHashMap<String, Object>();
        body.put("status", "ERROR");
        body.put("message", message);
        body.put("timestamp", Instant.now().toString());
        if (correlationId != null) body.put("correlationId", correlationId);
        return body;
    }
}
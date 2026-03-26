package com.tcs.fincore.ReportBuilder.controller;

import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;
import com.tcs.fincore.ReportBuilder.model.ReportViewRequest;
import com.tcs.fincore.ReportBuilder.exception.InfrastructureException;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.service.facade.ReportGenerationFacade;
import com.tcs.fincore.ReportBuilder.service.facade.dto.ReportFileResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/report-builder")
@RequiredArgsConstructor
@Slf4j
public class ReportController {

    private final ReportGenerationFacade reportFacade;

    /**
     * Synchronous report generation for immediate download
     */
    @PostMapping(value = "/view/{format}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> viewReport(
            @PathVariable String format, 
            @RequestBody ReportViewRequest request) {
        String correlationId = UUID.randomUUID().toString();
        String reportId = request != null ? request.getReportId() : null;

        log.info("View report request received: correlationId={} jobId={} reportId={} templateId={} variantCode={} format={}",
                correlationId, "N/A", reportId, request.getTemplateId(), request.getVariantCode(), format);

        try {
            ReportFileResponse response = reportFacade.generateReportFile(request, format);

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_TYPE, response.getMediaType());
            headers.setContentDisposition(ContentDisposition.attachment()
                    .filename(response.getFileName())
                    .build());

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(response.getContent());
                    
        } catch (ValidationException e) {
            log.warn("Validation failure in view report: correlationId={} jobId={} reportId={} templateId={}",
                    correlationId, "N/A", reportId, request != null ? request.getTemplateId() : null, e);
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", e.getMessage(),
                    "correlationId", correlationId
            ));
        } catch (RuntimeException e) {
            throw new InfrastructureException("Failed to generate report file", e);
        }
    }


    @PostMapping("/generate-for-hdfs")
    public ResponseEntity<?> generateForHdfs(@RequestBody ReportViewRequest request) {
        String correlationId = UUID.randomUUID().toString();
        String reportId = request != null ? request.getReportId() : null;
        log.info("Received single-mode HDFS generation request: reportId={}, templateId={}, variant={}",
                request.getReportId(), request.getTemplateId(), request.getVariantCode());

        try {
            // Validate required fields before handing off to async pipeline
            if ((request.getReportId() == null || request.getReportId().isBlank())
                    && (request.getTemplateId() == null || request.getTemplateId().isBlank())) {
                return ResponseEntity
                        .badRequest()
                        .body(Map.of(
                                "status", "ERROR",
                                "message", "Either reportId or templateId is required"
                        ));
            }

            // REST-triggered ? no Kafka lifecycle events will be published.
            // The facade generates a timestamp-based jobId internally from the context.
            ReportExecutionContext context = ReportExecutionContext.fromRest();

            reportFacade.generateAndStoreToHdfs(request, context);

            // Build a matching jobId for the response using the same pattern the facade uses
            String idForJob = (request.getReportId() != null && !request.getReportId().isBlank())
                    ? request.getReportId()
                    : request.getTemplateId();
            String jobId = "single_" + idForJob + "_" + System.currentTimeMillis();

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "ACCEPTED");
            response.put("message", "Single-mode report generation initiated asynchronously");
            response.put("jobId", jobId);
            response.put("reportId", request.getReportId());
            response.put("templateId", request.getTemplateId());
            response.put("variantCode", request.getVariantCode() != null ? request.getVariantCode() : "default");

            return ResponseEntity.accepted().body(response);

        } catch (ValidationException e) {
            log.warn("Validation failure for HDFS generation: correlationId={} jobId={} reportId={} templateId={}",
                    correlationId, "N/A", reportId, request != null ? request.getTemplateId() : null, e);
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", e.getMessage(),
                    "correlationId", correlationId
            ));
        } catch (TaskRejectedException e) {
            log.warn("Async executor saturated for single-mode HDFS generation: correlationId={} reportId={} templateId={}",
                    correlationId, reportId, request != null ? request.getTemplateId() : null, e);
            return ResponseEntity.status(503).body(Map.of(
                    "status", "ERROR",
                    "message", "Report generation queue is full. Please retry shortly.",
                    "correlationId", correlationId
            ));
        } catch (RuntimeException e) {
            throw new InfrastructureException("Failed to initiate single-mode HDFS report generation", e);
        }
    }



    /**
     *  NEW: Health check endpoint for report service
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
            "status", "UP",
            "service", "ReportBuilder",
            "version", "1.1"
        ));
    }
}

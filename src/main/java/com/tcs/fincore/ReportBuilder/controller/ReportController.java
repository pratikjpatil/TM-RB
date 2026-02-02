package com.tcs.fincore.ReportBuilder.controller;

import com.tcs.fincore.ReportBuilder.model.ReportViewRequest;
import com.tcs.fincore.ReportBuilder.service.facade.ReportGenerationFacade;
import com.tcs.fincore.ReportBuilder.service.facade.dto.ReportFileResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

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
        
        log.info("Received request to view report. TemplateId: {}, Variant: {}, Format: {}",
                request.getTemplateId(), request.getVariantCode(), format);

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
                    
        } catch (Exception e) {
            log.error("Report generation failed: {}", e.getMessage(), e);
            
            return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of(
                    "status", "ERROR",
                    "message", e.getMessage(),
                    "templateId", request.getTemplateId()
                ));
        }
    }

    /**
     *  Async HDFS generation - returns immediately
     *
     */
    @PostMapping("/generate-for-hdfs")
    public ResponseEntity<?> generateForHdfs(@RequestBody ReportViewRequest request) {
        log.info("Received request to store report in HDFS. TemplateId: {}, Variant: {}", 
            request.getTemplateId(), request.getVariantCode());

        try {
            // Validate request first
            if (request.getTemplateId() == null || request.getTemplateId().isBlank()) {
                return ResponseEntity
                    .badRequest()
                    .body(Map.of(
                        "status", "ERROR",
                        "message", "templateId is required"
                    ));
            }

            // Start async generation
            reportFacade.generateAndStoreToHdfs(request);

            // Generate job ID for tracking
            String jobId = "job_" + request.getTemplateId() + "_" + System.currentTimeMillis();

            return ResponseEntity.accepted()
                .body(Map.of(
                    "status", "ACCEPTED", 
                    "message", "Report generation initiated asynchronously",
                    "jobId", jobId,
                    "templateId", request.getTemplateId(),
                    "variantCode", request.getVariantCode() != null ? request.getVariantCode() : "default"
                ));
                
        } catch (Exception e) {
            log.error("Failed to initiate HDFS report generation: {}", e.getMessage(), e);
            
            return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of(
                    "status", "ERROR",
                    "message", "Failed to initiate report generation: " + e.getMessage(),
                    "templateId", request.getTemplateId()
                ));
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
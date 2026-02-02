package com.tcs.fincore.ReportBuilder.controller;

import com.tcs.fincore.ReportBuilder.batch.orchestrator.BatchJobOrchestrator;
import com.tcs.fincore.ReportBuilder.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.dto.BatchJobStatus;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for batch report generation
 * Handles submission, tracking, and management of batch jobs
 */
@RestController
@RequestMapping("/api/report-builder/batch")
@RequiredArgsConstructor
@Slf4j
public class BatchReportController {

    private final BatchJobOrchestrator orchestrator;

    @PostMapping("/submit")
    public ResponseEntity<?> submitBatchJob(@RequestBody BatchJobRequest request) {

        log.info("Received batch job submission: template={}, variant={}, reports={}",
                request.getTemplateId(),
                request.getVariantCode(),
                request.getVaryingDimension() != null ?
                        request.getVaryingDimension().getValues().size() : 0);

        try {
            // Submit job
            BatchJobStatus status = orchestrator.submitJob(request);

            // Return initial status
            return ResponseEntity.accepted().body(Map.of(
                    "status", "ACCEPTED",
                    "message", "Batch job submitted successfully",
                    "jobId", status.getJobId(),
                    "totalReports", status.getProgress().getTotalReports(),
                    "trackingUrl", "/report-builder/batch/status/" + status.getJobId()
            ));

        } catch (ValidationException e) {
            log.error("Validation failed: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", e.getMessage()
            ));

        } catch (Exception e) {
            log.error("Failed to submit batch job: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "message", "Failed to submit batch job: " + e.getMessage()
            ));
        }
    }

    /**
     * Get status of a batch job
     *
     * Returns detailed progress information including:
     * - Current phase
     * - Completed/failed report counts
     * - Error details
     * - Performance metrics
     */
    @GetMapping("/status/{jobId}")
    public ResponseEntity<?> getJobStatus(@PathVariable String jobId) {

        try {
            BatchJobStatus status = orchestrator.getJobStatus(jobId);
            return ResponseEntity.ok(status);

        } catch (ValidationException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "status", "ERROR",
                    "message", "Job not found: " + jobId
            ));

        } catch (Exception e) {
            log.error("Failed to get job status: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "message", "Failed to retrieve job status"
            ));
        }
    }

    /**
     * Cancel a running batch job
     */
    @PostMapping("/cancel/{jobId}")
    public ResponseEntity<?> cancelJob(@PathVariable String jobId) {

        log.info("Cancellation requested for job: {}", jobId);

        try {
            orchestrator.cancelJob(jobId);

            return ResponseEntity.ok(Map.of(
                    "status", "SUCCESS",
                    "message", "Job cancellation initiated",
                    "jobId", jobId
            ));

        } catch (ValidationException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "status", "ERROR",
                    "message", e.getMessage()
            ));

        } catch (Exception e) {
            log.error("Failed to cancel job: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "message", "Failed to cancel job"
            ));
        }
    }

    @PostMapping("/generate-all-branches")
    public ResponseEntity<?> generateAllBranchReports(
            @RequestBody Map<String, Object> request) {

        log.info("Received request to generate reports for all branches");

        try {
            String templateId = (String) request.get("templateId");
            String variantCode = (String) request.get("variantCode");
            String reportDate = (String) request.get("reportDate");

            if (templateId == null || templateId.isBlank()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "ERROR",
                        "message", "templateId is required"
                ));
            }

            // TODO: Fetch all active branch codes from database
            // For now, expecting them in request
            @SuppressWarnings("unchecked")
            java.util.List<String> branchCodes =
                    (java.util.List<String>) request.get("branchCodes");

            if (branchCodes == null || branchCodes.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "ERROR",
                        "message", "branchCodes list is required"
                ));
            }

            // Build batch request
            BatchJobRequest batchRequest = new BatchJobRequest();
            batchRequest.setTemplateId(templateId);
            batchRequest.setVariantCode(variantCode);
            batchRequest.setCommonParams(Map.of("reportDate", reportDate));

            BatchJobRequest.VaryingDimension dimension =
                    new BatchJobRequest.VaryingDimension();
            dimension.setParamName("branchCode");
            dimension.setValues(branchCodes);
            batchRequest.setVaryingDimension(dimension);

            batchRequest.setExecutionMode(BatchJobRequest.ExecutionMode.PARALLEL);
            batchRequest.setBatchSize(500);
            batchRequest.setParallelThreads(10);
            batchRequest.setOutputFormats(java.util.List.of("pdf", "xlsx"));

            // Submit job
            BatchJobStatus status = orchestrator.submitJob(batchRequest);

            return ResponseEntity.accepted().body(Map.of(
                    "status", "ACCEPTED",
                    "message", "Batch job submitted for all branches",
                    "jobId", status.getJobId(),
                    "totalBranches", branchCodes.size(),
                    "trackingUrl", "/report-builder/batch/status/" + status.getJobId()
            ));

        } catch (Exception e) {
            log.error("Failed to generate all branch reports: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "message", "Failed to submit job: " + e.getMessage()
            ));
        }
    }

    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "BatchReportGenerator",
                "version", "1.0"
        ));
    }
}
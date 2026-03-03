



package com.tcs.fincore.ReportBuilder.controller;

import com.tcs.fincore.ReportBuilder.batch.orchestrator.BatchJobOrchestrator;
import com.tcs.fincore.ReportBuilder.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.dto.BatchJobStatus;
import com.tcs.fincore.ReportBuilder.exception.InfrastructureException;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/report-builder/batch")
@RequiredArgsConstructor
@Slf4j
public class BatchReportController {

    private final BatchJobOrchestrator orchestrator;

    @PostMapping("/submit")
    public ResponseEntity<?> submitBatchJob(@RequestBody BatchJobRequest request) {

        String correlationId = UUID.randomUUID().toString();
        log.info("[{}] Received batch job submission: template={}, variant={}, reports={}",
                correlationId,
                request.getTemplateId(),
                request.getVariantCode(),
                request.getVaryingDimension() != null ?
                        request.getVaryingDimension().getValues().size() : 0);

        try {
            BatchJobStatus status = orchestrator.submitJob(request);
            return ResponseEntity.accepted().body(Map.of(
                    "status", "ACCEPTED",
                    "message", "Batch job submitted successfully",
                    "jobId", status.getJobId(),
                    "totalReports", status.getProgress().getTotalReports(),
                    "trackingUrl", "/api/report-builder/batch/status/" + status.getJobId()
            ));

        } catch (ValidationException e) {

            log.warn("Batch submission validation failed: correlationId={} jobId={} templateId={}",
                    correlationId, "N/A", request.getTemplateId(), e);
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", e.getMessage(),
                    "correlationId", correlationId
            ));

        } catch (RuntimeException e) {
            throw new InfrastructureException("Failed to submit batch job", e);
        }
    }

    @GetMapping("/status/{jobId}")
    public ResponseEntity<?> getJobStatus(@PathVariable String jobId) {


        if (jobId == null || !jobId.matches("[A-Za-z0-9_\\-]+")) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", "Invalid jobId format."
            ));
        }

        try {
            BatchJobStatus status = orchestrator.getJobStatus(jobId);
            return ResponseEntity.ok(status);

        } catch (ValidationException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "status", "ERROR",
                    "message", "Job not found."
            ));

        } catch (RuntimeException e) {
            throw new InfrastructureException("Failed to retrieve batch job status", e);
        }
    }

    @PostMapping("/cancel/{jobId}")
    public ResponseEntity<?> cancelJob(@PathVariable String jobId) {

        if (jobId == null || !jobId.matches("[A-Za-z0-9_\\-]+")) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", "Invalid jobId format."
            ));
        }

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

        } catch (RuntimeException e) {
            throw new InfrastructureException("Failed to cancel batch job", e);
        }
    }

    @PostMapping("/generate-all-branches")
    public ResponseEntity<?> generateAllBranchReports(@RequestBody Map<String, Object> request) {

        String correlationId = UUID.randomUUID().toString();
        log.info("[{}] Received request to generate reports for all branches", correlationId);

        try {
            String templateId = (String) request.get("templateId");
            String variantCode = (String) request.get("variantCode");
            String reportDate  = (String) request.get("reportDate");

            if (templateId == null || templateId.isBlank()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "ERROR", "message", "templateId is required"));
            }

            @SuppressWarnings("unchecked")
            List<String> branchCodes = (List<String>) request.get("branchCodes");

            if (branchCodes == null || branchCodes.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "ERROR", "message", "branchCodes list is required"));
            }

            for (String code : branchCodes) {
                if (code == null || !code.matches("[A-Za-z0-9_\\-]{1,20}")) {
                    return ResponseEntity.badRequest().body(Map.of(
                            "status", "ERROR",
                            "message", "Invalid branch code format. Codes must be alphanumeric (max 20 chars)."
                    ));
                }
            }

            // SECURITY FIX: Cap the number of branch codes to prevent DoS
            if (branchCodes.size() > 50_000) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "ERROR",
                        "message", "Too many branch codes (max: 50,000)."
                ));
            }

            BatchJobRequest batchRequest = new BatchJobRequest();
            batchRequest.setTemplateId(templateId);
            batchRequest.setVariantCode(variantCode);
            batchRequest.setCommonParams(Map.of("reportDate", reportDate));

            BatchJobRequest.VaryingDimension dimension = new BatchJobRequest.VaryingDimension();
            dimension.setParamName("branchCode");
            dimension.setValues(branchCodes);
            batchRequest.setVaryingDimension(dimension);

            batchRequest.setExecutionMode(BatchJobRequest.ExecutionMode.PARALLEL);
            batchRequest.setBatchSize(500);
            batchRequest.setParallelThreads(10);
            batchRequest.setOutputFormats(List.of("pdf", "xlsx"));

            BatchJobStatus status = orchestrator.submitJob(batchRequest);

            return ResponseEntity.accepted().body(Map.of(
                    "status", "ACCEPTED",
                    "message", "Batch job submitted for all branches",
                    "jobId", status.getJobId(),
                    "totalBranches", branchCodes.size(),
                    "trackingUrl", "/api/report-builder/batch/status/" + status.getJobId()
            ));

        } catch (ValidationException e) {
            log.warn("All-branches request validation failed: correlationId={} jobId={} templateId={}",
                    correlationId, "N/A", request.get("templateId"), e);
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR", "message", e.getMessage()));

        } catch (RuntimeException e) {
            throw new InfrastructureException("Failed to generate all-branch batch reports", e);
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "BatchReportGenerator",
                "version", "1.0"
        ));
    }
}

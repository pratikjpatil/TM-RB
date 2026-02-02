package com.tcs.fincore.ReportBuilder.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Request DTO for submitting batch report generation jobs
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BatchJobRequest {

    private String jobId;                // Optional: auto-generated if not provided
    private String templateId;           // Template to use
    private String variantCode;          // Variant code (e.g., BRANCH_WISE)

    // Common parameters shared across all reports (e.g., reportDate)
    private Map<String, Object> commonParams;

    // The dimension that varies across reports
    private VaryingDimension varyingDimension;

    // Execution settings
    private ExecutionMode executionMode = ExecutionMode.PARALLEL;
    private Integer batchSize = 500;     // Number of reports per batch
    private Integer parallelThreads = 10; // Parallel execution threads

    // Output settings
    private List<String> outputFormats;  // ["pdf", "xlsx", "txt"]
    private String hdfsBasePath;         // Optional: override default HDFS path

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VaryingDimension {
        private String paramName;        // e.g., "branchCode"
        private List<String> values;     // All values to generate reports for
    }

    public enum ExecutionMode {
        SEQUENTIAL,  // Process one by one (fallback)
        PARALLEL     // Process in parallel batches
    }
}
package com.tcs.fincore.ReportBuilder.batch.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

/**
 * Status tracking for batch report generation jobs
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchJobStatus {

    private String jobId;
    private String reportId;
    private String templateId;
    private String variantCode;

    private JobPhase currentPhase;
    private JobState status;

    private ProgressInfo progress;
    private ErrorInfo errors;

    private LocalDateTime startTime; //main job start time
    private LocalDateTime batchProcessStartTime; //batch process start time after materialization
    private LocalDateTime lastUpdateTime;
    private LocalDateTime completionTime;
    private Long estimatedTimeRemainingSeconds;

    public enum JobPhase {
        SUBMITTED,
        ANALYZING_TEMPLATE,
        MATERIALIZING_DATA,
        GENERATING_REPORTS,
        WRITING_TO_HDFS,
        COMPLETED,
        FAILED,
        CANCELLED
    }

    public enum JobState {
        PENDING,
        RUNNING,
        COMPLETED,
        FAILED,
        CANCELLED,
        PARTIALLY_COMPLETED
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProgressInfo {
        private int totalReports;
        private int completedReports;
        private int failedReports;
        private int skippedReports;
        private double percentageComplete;

        // Materialization progress (row-level)
//        private long estimatedMaterializationRows;
        private long materializedRows;

        // Track completed dimension values (e.g., branch codes)
        private Set<String> completedValues;
        private Set<String> failedValues;

        // Performance metrics
        private long reportsPerMinute;
        private long averageReportTimeMs;

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ErrorInfo {
        private int totalErrors;
        private List<ErrorDetail> recentErrors;
        private String lastErrorMessage;
        private LocalDateTime lastErrorTime;

        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class ErrorDetail {
            private String dimensionValue;  // e.g., branch code
            private String errorMessage;
            private String errorType;
            private LocalDateTime timestamp;
        }
    }
}
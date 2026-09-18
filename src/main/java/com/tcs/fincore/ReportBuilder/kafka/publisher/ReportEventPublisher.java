package com.tcs.fincore.ReportBuilder.kafka.publisher;

import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobStatus;
import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;

import java.time.Instant;
import java.time.LocalDateTime;

public interface ReportEventPublisher {

    // =========================================================================
    // Batch Mode (multi-report jobs)
    // =========================================================================

    /**
     * Publishes a JOB_STARTED lifecycle event for a batch-mode job.
     *
     * @param context trigger source and correlation IDs
     * @param status  current job status (must contain jobId, templateId, variantCode)
     */
    void publishBatchStarted(ReportExecutionContext context, BatchJobStatus status);

    /**
     * Publishes the terminal lifecycle event (COMPLETED or FAILED) for a batch-mode job.
     *
     * @param context      trigger source and correlation IDs
     * @param status       final job status
     * @param jobStartTime the instant the job was accepted, for duration calculation
     */
    void publishBatchTerminal(ReportExecutionContext context, BatchJobStatus status, LocalDateTime jobStartTime);

    // =========================================================================
    // Single Mode (single-report jobs)
    // =========================================================================

    /**
     * Publishes a JOB_STARTED lifecycle event for a single-mode report job.
     *
     * @param context     trigger source and correlation IDs
     * @param jobId       composite job identifier (processRunId_stageId_runId)
     * @param templateId  the report template being generated
     * @param variantCode the variant code (may be null)
     */
    void publishSingleStarted(ReportExecutionContext context,
                              String jobId,
                              String reportId,
                              String templateId,
                              String variantCode);

    /**
     * Publishes a JOB_COMPLETED lifecycle event for a single-mode report job.
     *
     * @param context      trigger source and correlation IDs
     * @param jobId        composite job identifier
     * @param templateId   the report template
     * @param variantCode  the variant code (may be null)
     * @param jobStartTime the instant the job was accepted, for duration calculation
     */
    void publishSingleCompleted(ReportExecutionContext context,
                                String jobId,
                                String reportId,
                                String templateId,
                                String variantCode,
                                LocalDateTime jobStartTime);

    void publishSingleFailed(ReportExecutionContext context,
                             String jobId,
                             String reportId,
                             String templateId,
                             String variantCode,
                             String errorMessage,
                             String errorType,
                             LocalDateTime jobStartTime);
}
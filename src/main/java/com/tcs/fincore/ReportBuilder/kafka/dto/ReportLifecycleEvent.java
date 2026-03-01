package com.tcs.fincore.ReportBuilder.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReportLifecycleEvent {

    // -- Correlation IDs (echoed from trigger) ----------------------------------
    String processRunId;
    String stageId;
    String runId;

    // -- Job identity -----------------------------------------------------------
    /** Composite job ID derived from the three correlation IDs. */
    String jobId;

    String templateId;
    String variantCode;

    // -- Event metadata ---------------------------------------------------------
    EventType eventType;
    JobStatus status;

    Instant eventTimestamp;
    Instant jobStartTime;
    Instant jobCompletionTime;

    // -- Optional detail --------------------------------------------------------
    Integer totalReports;
    Integer completedReports;
    Integer failedReports;
    Double percentageComplete;

    /** Populated only for FAILED events. */
    String errorMessage;
    String errorType;

    // -- Factory methods --------------------------------------------------------

    public static ReportLifecycleEvent started(
            ReportExecutionContext ctx,
            String jobId,
            String templateId,
            String variantCode) {

        return ReportLifecycleEvent.builder()
                .processRunId(ctx.getProcessRunId())
                .stageId(ctx.getStageId())
                .runId(ctx.getRunId())
                .jobId(jobId)
                .templateId(templateId)
                .variantCode(variantCode)
                .eventType(EventType.JOB_STARTED)
                .status(JobStatus.RUNNING)
                .eventTimestamp(Instant.now())
                .jobStartTime(Instant.now())
                .build();
    }

    public static ReportLifecycleEvent completed(
            ReportExecutionContext ctx,
            String jobId,
            String templateId,
            String variantCode,
            int totalReports,
            int completedReports,
            int failedReports,
            Instant startTime) {

        return ReportLifecycleEvent.builder()
                .processRunId(ctx.getProcessRunId())
                .stageId(ctx.getStageId())
                .runId(ctx.getRunId())
                .jobId(jobId)
                .templateId(templateId)
                .variantCode(variantCode)
                .eventType(EventType.JOB_COMPLETED)
                .status(failedReports > 0 ? JobStatus.PARTIALLY_COMPLETED : JobStatus.COMPLETED)
                .eventTimestamp(Instant.now())
                .jobStartTime(startTime)
                .jobCompletionTime(Instant.now())
                .totalReports(totalReports)
                .completedReports(completedReports)
                .failedReports(failedReports)
                .percentageComplete(totalReports > 0
                        ? (completedReports * 100.0) / totalReports : 0.0)
                .build();
    }

    public static ReportLifecycleEvent failed(
            ReportExecutionContext ctx,
            String jobId,
            String templateId,
            String variantCode,
            String errorMessage,
            String errorType,
            Instant startTime) {

        return ReportLifecycleEvent.builder()
                .processRunId(ctx.getProcessRunId())
                .stageId(ctx.getStageId())
                .runId(ctx.getRunId())
                .jobId(jobId)
                .templateId(templateId)
                .variantCode(variantCode)
                .eventType(EventType.JOB_FAILED)
                .status(JobStatus.FAILED)
                .eventTimestamp(Instant.now())
                .jobStartTime(startTime)
                .jobCompletionTime(Instant.now())
                .errorMessage(errorMessage)
                .errorType(errorType)
                .build();
    }

    /**
     * Returns the Kafka message key.
     * Using the composite ID ensures all events for a given run go to the same partition.
     */
    public String toKafkaMessageKey() {
        return String.join("_", processRunId, stageId, runId);
    }

    // -- Enums ------------------------------------------------------------------

    public enum EventType {
        JOB_STARTED,
        JOB_COMPLETED,
        JOB_FAILED
    }

    public enum JobStatus {
        RUNNING,
        COMPLETED,
        PARTIALLY_COMPLETED,
        FAILED,
        CANCELLED
    }
}
package com.tcs.fincore.ReportBuilder.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.time.LocalDateTime;

@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReportLifecycleEvent {

    // -- Correlation IDs (echoed from trigger) ----------------------------------
    String processRunId;
    String stageId;
    String runId;
    String reportType;

    // -- Job identity -----------------------------------------------------------
    /** Composite job ID derived from the three correlation IDs. */
    String jobId;

    String reportId;
    String templateId;
    String variantCode;

    // -- Event metadata ---------------------------------------------------------
//    EventType eventType;
    JobStatus status;
    String remarks;

    Instant eventTimestamp;
//    Instant jobStartTime;
//    Instant jobCompletionTime;

    LocalDateTime startTime;
    LocalDateTime endTime;


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
            String reportId,
            String templateId,
            String variantCode,
            String remarks) {

        return ReportLifecycleEvent.builder()
                .processRunId(ctx.getProcessRunId())
                .stageId(ctx.getStageId())
                .runId(ctx.getRunId())
                .jobId(jobId)
                .reportId(reportId)
                .templateId(templateId)
                .variantCode(variantCode)
//                .eventType(EventType.JOB_STARTED)
                .remarks(remarks)
                .status(JobStatus.STARTED)
                .eventTimestamp(Instant.now())
                .startTime(LocalDateTime.now())
                .build();
    }

    public static ReportLifecycleEvent completed(
            ReportExecutionContext ctx,
            String jobId,
            String reportId,
            String templateId,
            String variantCode,
            int totalReports,
            int completedReports,
            int failedReports,
            LocalDateTime startTime,
            String remarks) {

        return ReportLifecycleEvent.builder()
                .processRunId(ctx.getProcessRunId())
                .stageId(ctx.getStageId())
                .runId(ctx.getRunId())
                .jobId(jobId)
                .reportId(reportId)
                .templateId(templateId)
                .variantCode(variantCode)
//                .eventType(EventType.JOB_COMPLETED)
                .status(failedReports > 0 ? JobStatus.FAILED : JobStatus.COMPLETED)
                .remarks(remarks)
                .eventTimestamp(Instant.now())
                .startTime(startTime)
                .endTime(LocalDateTime.now())
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
            String reportId,
            String templateId,
            String variantCode,
            String errorMessage,
            String errorType,
            LocalDateTime startTime,
            String remarks) {

        return ReportLifecycleEvent.builder()
                .processRunId(ctx.getProcessRunId())
                .stageId(ctx.getStageId())
                .runId(ctx.getRunId())
                .jobId(jobId)
                .reportId(reportId)
                .templateId(templateId)
                .variantCode(variantCode)
//                .eventType(EventType.JOB_FAILED)
                .remarks(remarks)
                .status(JobStatus.FAILED)
                .eventTimestamp(Instant.now())
                .startTime(startTime)
                .endTime(LocalDateTime.now())
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

    public enum JobStatus {
        STARTED,
        COMPLETED,
        FAILED
    }
}
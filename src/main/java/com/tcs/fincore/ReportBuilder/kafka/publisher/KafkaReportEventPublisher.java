package com.tcs.fincore.ReportBuilder.kafka.publisher;

import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobStatus;
import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;
import com.tcs.fincore.ReportBuilder.kafka.dto.ReportLifecycleEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka-backed implementation of {@link ReportEventPublisher}.
 *
 * <p>Publishes {@link ReportLifecycleEvent} messages to the configured Kafka topics.
 * Events are sent asynchronously (fire-and-forget with logging on failure) so that
 * Kafka unavailability never blocks the report generation pipeline.
 *
 * <p>Events are only published when the {@link ReportExecutionContext} identifies
 * the job as Kafka-triggered — REST-triggered jobs are silently skipped.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaReportEventPublisher implements ReportEventPublisher {

    private final KafkaTemplate<String, ReportLifecycleEvent> kafkaTemplate;

    // -- Topic configuration --------------------------------------------------

    @Value("${kafka.topics.report-started:report-builder.batch-mode.started}")
    private String batchStartedTopic;

    @Value("${kafka.topics.report-completed:report-builder.batch-mode.completed}")
    private String batchCompletedTopic;

    @Value("${kafka.topics.single-report-started:report-builder.single-mode.started}")
    private String singleStartedTopic;

    @Value("${kafka.topics.single-report-completed:report-builder.single-mode.completed}")
    private String singleCompletedTopic;

    // =========================================================================
    // Batch Mode
    // =========================================================================

    @Override
    public void publishBatchStarted(ReportExecutionContext context, BatchJobStatus status) {
        if (!shouldPublish(context)) return;
        String remark = String.format("RB batch generation started for templateId: %s, reportId: %s, variantCode: %s.", status.getTemplateId(), status.getReportId(), status.getVariantCode());
        ReportLifecycleEvent event = ReportLifecycleEvent.started(
                context,
                status.getJobId(),
                status.getReportId(),
                status.getTemplateId(),
                status.getVariantCode(),
                remark
                );

        send(batchStartedTopic, event);
    }

    @Override
    public void publishBatchTerminal(ReportExecutionContext context,
                                     BatchJobStatus status,
                                     LocalDateTime jobStartTime) {
        if (!shouldPublish(context)) return;

        ReportLifecycleEvent event = buildBatchTerminalEvent(context, status, jobStartTime);
        send(batchCompletedTopic, event);
    }

    // =========================================================================
    // Single Mode
    // =========================================================================

    @Override
    public void publishSingleStarted(ReportExecutionContext context,
                                     String jobId,
                                     String reportId,
                                     String templateId,
                                     String variantCode) {
        if (!shouldPublish(context)) return;
        String remark = String.format("RB single generation started for templateId: %s, reportId: %s, variantCode: %s.", templateId, reportId, variantCode);
        ReportLifecycleEvent event = ReportLifecycleEvent.started(
                context, jobId, reportId, templateId, variantCode, remark);

        send(singleStartedTopic, event);
    }

    @Override
    public void publishSingleCompleted(ReportExecutionContext context,
                                       String jobId,
                                       String reportId,
                                       String templateId,
                                       String variantCode,
                                       LocalDateTime jobStartTime) {
        if (!shouldPublish(context)) return;
        String remark = String.format("RB single generation completed for templateId: %s, reportId: %s, variantCode: %s.", templateId, reportId, variantCode);
        // Single mode = exactly 1 report, no partial completions
        ReportLifecycleEvent event = ReportLifecycleEvent.completed(
                context, jobId, reportId, templateId, variantCode,
                /* totalReports   */ 1,
                /* completedReports */ 1,
                /* failedReports  */ 0,
                jobStartTime,
                remark);

        send(singleCompletedTopic, event);
    }

    @Override
    public void publishSingleFailed(ReportExecutionContext context,
                                    String jobId,
                                    String reportId,
                                    String templateId,
                                    String variantCode,
                                    String errorMessage,
                                    String errorType,
                                    LocalDateTime jobStartTime) {
        if (!shouldPublish(context)) return;
        String remark = String.format("RB single generation failed for reportId: %s, templateId: %s, err : %s, errType : %s", reportId, templateId, errorMessage, errorType);
        ReportLifecycleEvent event = ReportLifecycleEvent.failed(
                context, jobId, reportId, templateId, variantCode,
                errorMessage, errorType, jobStartTime, remark);

        send(singleCompletedTopic, event);
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    private boolean shouldPublish(ReportExecutionContext context) {
        if (context == null || !context.isKafkaTriggered()) {
            log.debug("Skipping event publish — trigger source is not KAFKA");
            return false;
        }
        return true;
    }

    /**
     * Sends {@code event} to {@code topic} asynchronously.
     * Failures are logged as errors but never propagate to the caller — Kafka unavailability
     * must not block or fail the report generation pipeline.
     */
    private void send(String topic, ReportLifecycleEvent event) {
        String messageKey = event.toKafkaMessageKey();
        CompletableFuture<SendResult<String, ReportLifecycleEvent>> future =
                kafkaTemplate.send(topic, messageKey, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish {} event to topic '{}' for jobId={}: {}",
                        event.getStatus(), topic, event.getJobId(), ex.getMessage(), ex);
            } else {
                log.debug("Published {} event to topic '{}' partition={} offset={} jobId={}",
                        event.getStatus(), topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.getJobId());
            }
        });
    }

    /**
     * Builds the correct terminal event for a batch job based on its final state.
     */
    private ReportLifecycleEvent buildBatchTerminalEvent(ReportExecutionContext context,
                                                         BatchJobStatus status,
                                                         LocalDateTime jobStartTime) {
        if (status.getStatus() == BatchJobStatus.JobState.FAILED) {
            String errorMessage = status.getErrors() != null ? status.getErrors().getLastErrorMessage() : "Unknown error";
            String errorType = "BatchJobFailure";
            String remark = String.format("RB batch generation failed for reportId: %s, templateId: %s, err: %s, errType: %s", status.getReportId(), status.getTemplateId(), errorMessage, errorType);

            return ReportLifecycleEvent.failed(
                    context,
                    status.getJobId(),
                    status.getReportId(),
                    status.getTemplateId(),
                    status.getVariantCode(),
                    errorMessage,
                    errorType,
                    jobStartTime,
                    remark);
        }

        BatchJobStatus.ProgressInfo progress = status.getProgress();
        int total     = progress != null ? progress.getTotalReports()     : 0;
        int completed = progress != null ? progress.getCompletedReports() : 0;
        int failed    = progress != null ? progress.getFailedReports()    : 0;

        String remark = String.format("RB batch generation completed for reportId: %s, templateId: %s, failedReports: %d, completedReports: %d, totalReports: %d.", status.getReportId(), status.getTemplateId(), failed, completed, total);

        return ReportLifecycleEvent.completed(
                context,
                status.getJobId(),
                status.getReportId(),
                status.getTemplateId(),
                status.getVariantCode(),
                total, completed, failed,
                jobStartTime, remark);
    }
}
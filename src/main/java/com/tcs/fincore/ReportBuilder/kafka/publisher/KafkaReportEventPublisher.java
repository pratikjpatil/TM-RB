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

import java.time.Instant;
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

        ReportLifecycleEvent event = ReportLifecycleEvent.started(
                context,
                status.getJobId(),
                status.getTemplateId(),
                status.getVariantCode());

        send(batchStartedTopic, event);
    }

    @Override
    public void publishBatchTerminal(ReportExecutionContext context,
                                     BatchJobStatus status,
                                     Instant jobStartTime) {
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
                                     String templateId,
                                     String variantCode) {
        if (!shouldPublish(context)) return;

        ReportLifecycleEvent event = ReportLifecycleEvent.started(
                context, jobId, templateId, variantCode);

        send(singleStartedTopic, event);
    }

    @Override
    public void publishSingleCompleted(ReportExecutionContext context,
                                       String jobId,
                                       String templateId,
                                       String variantCode,
                                       Instant jobStartTime) {
        if (!shouldPublish(context)) return;

        // Single mode = exactly 1 report, no partial completions
        ReportLifecycleEvent event = ReportLifecycleEvent.completed(
                context, jobId, templateId, variantCode,
                /* totalReports   */ 1,
                /* completedReports */ 1,
                /* failedReports  */ 0,
                jobStartTime);

        send(singleCompletedTopic, event);
    }

    @Override
    public void publishSingleFailed(ReportExecutionContext context,
                                    String jobId,
                                    String templateId,
                                    String variantCode,
                                    String errorMessage,
                                    String errorType,
                                    Instant jobStartTime) {
        if (!shouldPublish(context)) return;

        ReportLifecycleEvent event = ReportLifecycleEvent.failed(
                context, jobId, templateId, variantCode,
                errorMessage, errorType, jobStartTime);

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
                        event.getEventType(), topic, event.getJobId(), ex.getMessage(), ex);
            } else {
                log.debug("Published {} event to topic '{}' partition={} offset={} jobId={}",
                        event.getEventType(), topic,
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
                                                         Instant jobStartTime) {
        if (status.getStatus() == BatchJobStatus.JobState.FAILED) {
            return ReportLifecycleEvent.failed(
                    context,
                    status.getJobId(),
                    status.getTemplateId(),
                    status.getVariantCode(),
                    status.getErrors() != null ? status.getErrors().getLastErrorMessage() : "Unknown error",
                    "BatchJobFailure",
                    jobStartTime);
        }

        BatchJobStatus.ProgressInfo progress = status.getProgress();
        int total     = progress != null ? progress.getTotalReports()     : 0;
        int completed = progress != null ? progress.getCompletedReports() : 0;
        int failed    = progress != null ? progress.getFailedReports()    : 0;

        return ReportLifecycleEvent.completed(
                context,
                status.getJobId(),
                status.getTemplateId(),
                status.getVariantCode(),
                total, completed, failed,
                jobStartTime);
    }
}
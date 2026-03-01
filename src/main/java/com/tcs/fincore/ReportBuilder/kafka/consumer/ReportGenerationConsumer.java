package com.tcs.fincore.ReportBuilder.kafka.consumer;

import com.tcs.fincore.ReportBuilder.batch.orchestrator.BatchJobOrchestrator;
import com.tcs.fincore.ReportBuilder.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.dto.BatchJobStatus;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;
import com.tcs.fincore.ReportBuilder.kafka.dto.KafkaReportTriggerMessage;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReportGenerationConsumer {

    private final BatchJobOrchestrator orchestrator;
    private final Validator validator;

    @KafkaListener(
            topics        = "${kafka.topics.report-trigger:report-builder.batch-mode.trigger}",
            groupId       = "${kafka.consumer.group-id:report-builder-group}",
            containerFactory = "reportGenerationListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, KafkaReportTriggerMessage> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        String kafkaOffset = String.format("partition=%d offset=%d", partition, offset);
        KafkaReportTriggerMessage message = record.value();

        log.info("Received report trigger message: key={} {} processRunId={} stageId={} runId={}",
                record.key(), kafkaOffset,
                message.getProcessRunId(), message.getStageId(), message.getRunId());

        // -- Step 1: Validate ----------------------------------------------------
        Set<ConstraintViolation<KafkaReportTriggerMessage>> violations = validator.validate(message);
        if (!violations.isEmpty()) {
            String errors = violations.stream()
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .collect(Collectors.joining(", "));
            log.error("Invalid Kafka message ({}): {}. Acknowledging to prevent redelivery loop.",
                    kafkaOffset, errors);
            // Commit offset — bad message should not block the partition
            ack.acknowledge();
            return;
        }

        // -- Step 2: Build execution context ------------------------------------
        ReportExecutionContext context = ReportExecutionContext.fromKafka(
                message.getProcessRunId(),
                message.getStageId(),
                message.getRunId(),
                kafkaOffset);

        // -- Step 3: Prepare request with composite job ID ----------------------
        BatchJobRequest request = message.getPayload();
        String compositeJobId = message.toCompositeJobId();
        request.setJobId(compositeJobId);

        log.info("Submitting Kafka-triggered batch job: jobId={} templateId={} variant={}",
                compositeJobId, request.getTemplateId(), request.getVariantCode());

        // -- Step 4: Submit to orchestrator -------------------------------------
        try {
            BatchJobStatus status = orchestrator.submitJob(request, context);
            log.info("Batch job accepted: jobId={} totalReports={}",
                    status.getJobId(),
                    status.getProgress() != null ? status.getProgress().getTotalReports() : "?");

            // Acknowledge AFTER successful submission (job is now tracked internally)
            ack.acknowledge();

        } catch (ValidationException e) {
            // Request content is invalid — ack to prevent infinite retry of same bad request
            log.error("Validation error for Kafka-triggered job ({}): {}", kafkaOffset, e.getMessage());
            ack.acknowledge();

        } catch (Exception e) {
            // Transient/infrastructure failure — do NOT ack; Kafka will redeliver
            log.error("Failed to submit Kafka-triggered job ({}): {}", kafkaOffset, e.getMessage(), e);
            // Spring Kafka's SeekToCurrentErrorHandler / DefaultErrorHandler will handle retries + DLT
            throw new RuntimeException("Failed to submit report job — will retry", e);
        }
    }
}
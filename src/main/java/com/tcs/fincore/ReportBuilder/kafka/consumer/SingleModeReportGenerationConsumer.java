package com.tcs.fincore.ReportBuilder.kafka.consumer;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;
import com.tcs.fincore.ReportBuilder.kafka.dto.KafkaSingleReportTriggerMessage;
import com.tcs.fincore.ReportBuilder.model.ReportViewRequest;
import com.tcs.fincore.ReportBuilder.service.facade.ReportGenerationFacade;
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
public class SingleModeReportGenerationConsumer {

    private final ReportGenerationFacade reportGenerationFacade;
    private final Validator validator;

    @KafkaListener(
            topics           = "${kafka.topics.single-report-trigger:report-builder.single-mode.trigger}",
            groupId          = "${kafka.consumer.group-id:report-builder-group}",
            containerFactory = "singleModeReportListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, KafkaSingleReportTriggerMessage> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        String kafkaOffset = String.format("partition=%d offset=%d", partition, offset);
        KafkaSingleReportTriggerMessage message = record.value();

        log.info("Received single-mode report trigger: key={} {} processRunId={} stageId={} runId={}",
                record.key(), kafkaOffset,
                message.getProcessRunId(), message.getStageId(), message.getRunId());

        // -- Step 1: Bean validation -------------------------------------------
        Set<ConstraintViolation<KafkaSingleReportTriggerMessage>> violations =
                validator.validate(message);

        if (!violations.isEmpty()) {
            String errors = violations.stream()
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .collect(Collectors.joining(", "));
            log.error("Invalid single-mode Kafka message ({}): {}. Acknowledging to prevent redelivery loop.",
                    kafkaOffset, errors);
            ack.acknowledge();
            return;
        }

        // -- Step 2: Build execution context from correlation IDs --------------
        ReportExecutionContext context = ReportExecutionContext.fromKafka(
                message.getProcessRunId(),
                message.getStageId(),
                message.getRunId(),
                kafkaOffset);

        // -- Step 3: Extract payload (jobId is derived from context inside the facade) ---
        // The composite jobId lives in ReportExecutionContext (processRunId_stageId_runId).
        // We deliberately do NOT set it on the request to avoid adding fields to ReportViewRequest.
        ReportViewRequest request = message.getPayload();
        String compositeJobId = message.toCompositeJobId();

        log.info("Submitting Kafka-triggered single-mode job: jobId={} reportId={} templateId={} variant={}",
                compositeJobId, request.getReportId(), request.getTemplateId(), request.getVariantCode());

        // -- Step 4: Delegate to facade (runs @Async) -------------------------
        try {
            reportGenerationFacade.generateAndStoreToHdfs(request, context);

            // Ack AFTER successful submission (job is tracked internally via the composite ID)
            ack.acknowledge();

            log.info("Single-mode job accepted and submitted asynchronously: jobId={}", compositeJobId);

        } catch (ValidationException e) {
            // Request content is invalid — ack to prevent infinite retry of the same bad payload
            log.error("Validation error for single-mode Kafka job ({}): {}", kafkaOffset, e.getMessage());
            ack.acknowledge();

        } catch (RuntimeException e) {
            // Transient/infrastructure failure — do NOT ack; let DefaultErrorHandler retry + DLT
            log.error("Failed to submit single-mode report job ({}): {}", kafkaOffset, e.getMessage(), e);
            throw new RuntimeException("Failed to submit single-mode report job — will retry", e);
        }
    }
}
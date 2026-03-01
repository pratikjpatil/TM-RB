package com.tcs.fincore.ReportBuilder.kafka.context;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ReportExecutionContext {

    /** Trigger source — drives whether lifecycle events are published. */
    TriggerSource triggerSource;

    /** Upstream process-run identifier. Null for REST-triggered jobs. */
    String processRunId;

    /** Upstream stage identifier. Null for REST-triggered jobs. */
    String stageId;

    /** Upstream run identifier. Null for REST-triggered jobs. */
    String runId;

    /** Kafka partition + offset for traceability. Optional. */
    String kafkaOffset;

    /**
     * Convenience factory: create a context for a Kafka-triggered job.
     */
    public static ReportExecutionContext fromKafka(
            String processRunId,
            String stageId,
            String runId,
            String kafkaOffset) {
        return ReportExecutionContext.builder()
                .triggerSource(TriggerSource.KAFKA)
                .processRunId(processRunId)
                .stageId(stageId)
                .runId(runId)
                .kafkaOffset(kafkaOffset)
                .build();
    }

    /**
     * Convenience factory: create a context for a REST-triggered job.
     * No correlation IDs and no lifecycle events will be published.
     */
    public static ReportExecutionContext fromRest() {
        return ReportExecutionContext.builder()
                .triggerSource(TriggerSource.REST)
                .build();
    }

    /**
     * Returns true when lifecycle events should be published to Kafka topics.
     */
    public boolean isKafkaTriggered() {
        return TriggerSource.KAFKA == triggerSource;
    }

    /**
     * Enumerates all possible trigger sources.
     * Extend here when new ingress channels (e.g. gRPC, scheduler) are added.
     */
    public enum TriggerSource {
        KAFKA,
        REST
    }
}
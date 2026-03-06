package com.tcs.fincore.ReportBuilder.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaReportTriggerMessage {

    /**
     * Process-run identifier from the upstream orchestrator.
     * Combined with stageId and runId to form the composite batchId.
     */
    @NotBlank(message = "processRunId must not be blank")
    private String processRunId;

    /**
     * Stage identifier within the process run.
     */
    @NotBlank(message = "stageId must not be blank")
    private String stageId;

    /**
     * Unique run identifier for this particular execution.
     */
    @NotBlank(message = "runId must not be blank")
    private String runId;

    /**
     * The actual batch generation request payload.
     * Contains all BatchJobRequest fields except jobId (overridden by correlation IDs).
     */
    @NotNull(message = "payload must not be null")
    @Valid
    private BatchJobRequest payload;

    /**
     * Builds the composite batchId from the three correlation IDs.
     * This is the canonical identifier for the job across all systems.
     */
    public String toCompositeJobId() {
        return String.join("_", processRunId, stageId, runId);
    }
}
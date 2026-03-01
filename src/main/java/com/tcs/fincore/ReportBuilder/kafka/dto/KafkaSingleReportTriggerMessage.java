package com.tcs.fincore.ReportBuilder.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.tcs.fincore.ReportBuilder.model.ReportViewRequest;
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
public class KafkaSingleReportTriggerMessage {

    /**
     * Process-run identifier from the upstream orchestrator.
     * Combined with {@code stageId} and {@code runId} to form the composite jobId.
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
     * The single-report generation request payload.
     * Contains the templateId, variantCode, and runtime params.
     */
    @NotNull(message = "payload must not be null")
    @Valid
    private ReportViewRequest payload;

    /**
     * Builds the composite jobId from the three correlation IDs.
     * This is the canonical identifier for the job across all systems.
     */
    public String toCompositeJobId() {
        return String.join("_", processRunId, stageId, runId);
    }
}
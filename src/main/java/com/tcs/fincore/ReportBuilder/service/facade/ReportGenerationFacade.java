


package com.tcs.fincore.ReportBuilder.service.facade;

import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;
import com.tcs.fincore.ReportBuilder.model.ReportViewRequest;
import com.tcs.fincore.ReportBuilder.service.facade.dto.ReportFileResponse;

import java.util.concurrent.CompletableFuture;

public interface ReportGenerationFacade {

    /**
     * Generates a single report in memory and returns it for immediate download or viewing.
     *
     * <p>This is a synchronous operation intended for REST-triggered requests only.
     * No Kafka lifecycle events are published.
     *
     * @param request    the report generation request (templateId, variantCode, params)
     * @param formatType the desired export format (e.g., "pdf", "xlsx")
     * @return the generated report content along with filename and media type
     */
    ReportFileResponse generateReportFile(ReportViewRequest request, String formatType);

    CompletableFuture<Void> generateAndStoreToHdfs(ReportViewRequest request, ReportExecutionContext context);
}

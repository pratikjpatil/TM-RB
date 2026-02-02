package com.tcs.fincore.ReportBuilder.service.facade;

import com.tcs.fincore.ReportBuilder.model.ReportViewRequest;
import com.tcs.fincore.ReportBuilder.service.facade.dto.ReportFileResponse;

public interface ReportGenerationFacade {
    /**
     * Generates a report in memory for immediate download/viewing.
     */
    ReportFileResponse generateReportFile(ReportViewRequest request, String formatType);

    /**
     * Generates all formats of the report and persists them to HDFS.
     */
    void generateAndStoreToHdfs(ReportViewRequest request);
}
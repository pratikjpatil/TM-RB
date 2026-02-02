package com.tcs.fincore.ReportBuilder.service.export;

import com.tcs.fincore.ReportBuilder.model.ExportModel;

public interface ReportExporter {
    byte[] generate(ExportModel model);
    String getMediaType();
    String getFileExtension();
    boolean supports(String formatType);
}
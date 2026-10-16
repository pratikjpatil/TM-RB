package com.tcs.fincore.ReportBuilder.service.metadata;

import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;

public interface ReportMetadataService {
    ReportTemplate getTemplate(String templateId);
    ReportTemplate getTemplateByReportId(String reportId);
    ReportVariant getVariant(String templateId, String variantCode);
}
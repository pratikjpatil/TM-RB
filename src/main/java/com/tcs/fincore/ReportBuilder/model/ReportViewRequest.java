package com.tcs.fincore.ReportBuilder.model;

import lombok.Data;

import java.util.Map;

/**
 * Request used by "viewer" to generate a report.
 */
@Data
public class ReportViewRequest {
    private String reportId;             // REPORT_ID in RB_REPORT_TEMPLATE
    private String templateId;           // TEMPLATE_ID in RB_REPORT_TEMPLATE
    private String variantCode;          // WHOLE_BANK, BRANCH_WISE, etc.
    private Map<String, Object> params;  // paramName -> value (branchCode, reportDate, etc.)
}

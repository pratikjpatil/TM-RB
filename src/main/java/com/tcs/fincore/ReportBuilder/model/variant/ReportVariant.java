package com.tcs.fincore.ReportBuilder.model.variant;

import lombok.Data;

import java.util.List;

/**
 * ReportVariant - one viewing mode of a template, e.g. WHOLE_BANK, BRANCH_WISE.
 */
@Data
public class ReportVariant {
    private String variantId;      // PK in RB_REPORT_VARIANT
    private String templateId;     // FK to RB_REPORT_TEMPLATE
    private String variantCode;    // WHOLE_BANK, BRANCH_WISE...
    private String variantName;    // "Whole Bank"
    private String description;
    private String status;         // ACTIVE / INACTIVE

    private List<VariantParamDef> params;
    private List<FilterRule> filterRules;
}
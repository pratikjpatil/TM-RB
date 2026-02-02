
package com.tcs.fincore.ReportBuilder.model.variant;

import lombok.Data;

/**
 * VariantParamDef - describes a parameter the viewer must/ may provide.
 */
@Data
public class VariantParamDef {
    private Long paramId;
    private String variantId;

    private String paramName;     // branchCode, reportDate, circleCode...
    private String label;         // for UI
    private String paramType;     // STRING, NUMBER, DATE, LIST
    private boolean required;
    private boolean multiValued;
    private String validationRegex;
    private String uiHint;        // textbox, dropdown, datepicker...
}
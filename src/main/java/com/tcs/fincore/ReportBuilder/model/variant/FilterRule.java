
package com.tcs.fincore.ReportBuilder.model.variant;

import lombok.Data;

/**
 * FilterRule - describes how a runtime param is applied as a DB filter.
 */
@Data
public class FilterRule {
    private Long ruleId;
    private String variantId;

    private String scopeType;   // ALL_DB, TABLE, DYNAMIC_TABLE, TAG
    private String scopeValue;  // table name or tag

    private String paramName;   // branchCode, circleCode, reportDate...
    private String dbColumn;    // BRANCH_CODE, CIRCLE_CODE, REPORT_DATE...
    private String operator;    // =, >=, <=, IN, BETWEEN, LIKE, >, <
}
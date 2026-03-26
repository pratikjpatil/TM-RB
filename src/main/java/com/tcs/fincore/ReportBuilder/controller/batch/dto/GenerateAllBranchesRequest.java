package com.tcs.fincore.ReportBuilder.controller.batch.dto;

import java.util.List;

public record GenerateAllBranchesRequest(
        String reportId,
        String templateId,
        String variantCode,
        String reportDate,
        List<String> branchCodes
) {
}

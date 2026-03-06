package com.tcs.fincore.ReportBuilder.batch.orchestrator.model;

import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;

import java.util.List;
import java.util.Map;

public record BatchAnalysisResult(
        ReportTemplate template,
        ReportVariant variant,
        List<DataSourceSpec> allSpecs,
        List<QueryPattern> patterns,
        boolean useBatchMode,
        Map<String, Object> staticContext,
        Map<String, Map<String, Object>> preFetchedGlobals
) {
}

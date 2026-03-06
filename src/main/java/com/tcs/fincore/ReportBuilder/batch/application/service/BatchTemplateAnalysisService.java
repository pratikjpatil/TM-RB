package com.tcs.fincore.ReportBuilder.batch.application.service;

import com.tcs.fincore.ReportBuilder.batch.analyzer.QueryPatternAnalyzer;
import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.batch.application.model.BatchAnalysisResult;
import com.tcs.fincore.ReportBuilder.batch.application.port.BatchTemplateAnalyzer;
import com.tcs.fincore.ReportBuilder.batch.processor.GlobalParamBatchProcessor;
import com.tcs.fincore.ReportBuilder.batch.application.util.TemplateCloner;
import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import com.tcs.fincore.ReportBuilder.service.metadata.ReportMetadataService;
import com.tcs.fincore.ReportBuilder.service.runtime.GlobalParamEngine;
import com.tcs.fincore.ReportBuilder.service.runtime.ReportRuntimeService;
import com.tcs.fincore.ReportBuilder.service.template.TemplateProcessorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchTemplateAnalysisService implements BatchTemplateAnalyzer {

    private final ReportMetadataService metadataService;
    private final ReportRuntimeService runtimeService;
    private final TemplateProcessorService templateProcessor;
    private final QueryPatternAnalyzer patternAnalyzer;
    private final GlobalParamEngine globalParamEngine;
    private final GlobalParamBatchProcessor globalParamBatchProcessor;
    private final TemplateCloner templateCloner;

    public BatchAnalysisResult analyze(BatchJobRequest request) {
        log.info("Phase 1: Analyzing template {}", request.getTemplateId());
        long startTime = System.currentTimeMillis();

        ReportTemplate rawTemplate = metadataService.getTemplate(request.getTemplateId());
        ReportTemplate structuredTemplate = templateCloner.deepClone(rawTemplate);

        Map<String, Object> staticContext = globalParamEngine.resolveContext(
                structuredTemplate, request.getCommonParams(), false);

        Map<String, Map<String, Object>> preFetchedGlobals =
                globalParamBatchProcessor.prefetchDynamicGlobals(
                        structuredTemplate,
                        staticContext,
                        request.getVaryingDimension().getParamName(),
                        request.getVaryingDimension().getValues());

        templateProcessor.validate(structuredTemplate);
        templateProcessor.expandDynamicRows(structuredTemplate);
        templateProcessor.assignIds(structuredTemplate);

        ReportTemplate analysisTemplate = templateCloner.deepClone(structuredTemplate);
        ReportVariant variant = null;
        if (request.getVariantCode() != null && !request.getVariantCode().isBlank()) {
            variant = metadataService.getVariant(request.getTemplateId(), request.getVariantCode());
        }
        if (variant != null) {
            runtimeService.applyBatchParams(analysisTemplate, variant,
                    request.getCommonParams(),
                    request.getVaryingDimension().getParamName(),
                    preFetchedGlobals);
        }

        List<DataSourceSpec> allSpecs = templateProcessor.extractDbSpecs(analysisTemplate);
        List<QueryPattern> patterns = patternAnalyzer.analyzeTemplate(
                analysisTemplate, variant, request, allSpecs);
        boolean useBatchMode = patternAnalyzer.shouldUseBatchMode(request, patterns);

        log.info("Phase 1 complete in {}ms: useBatchMode={}, patterns={}, specs={}",
                System.currentTimeMillis() - startTime, useBatchMode,
                patterns.size(), allSpecs.size());

        return new BatchAnalysisResult(structuredTemplate, variant, allSpecs,
                patterns, useBatchMode, staticContext, preFetchedGlobals);
    }
}

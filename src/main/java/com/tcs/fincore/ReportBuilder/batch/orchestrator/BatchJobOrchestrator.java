package com.tcs.fincore.ReportBuilder.batch.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.batch.analyzer.QueryPatternAnalyzer;
import com.tcs.fincore.ReportBuilder.batch.distributor.ResultDistributor;
import com.tcs.fincore.ReportBuilder.batch.materializer.BatchDataMaterializer;
import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.batch.processor.GlobalParamBatchProcessor;
import com.tcs.fincore.ReportBuilder.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.dto.BatchJobStatus;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.ReportViewRequest;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.model.variant.VariantParamDef;
import com.tcs.fincore.ReportBuilder.service.aggregation.AggregatorService;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import com.tcs.fincore.ReportBuilder.service.calculation.CalculationService;
import com.tcs.fincore.ReportBuilder.service.export.ExporterFactory;
import com.tcs.fincore.ReportBuilder.service.export.ReportExporter;
import com.tcs.fincore.ReportBuilder.service.metadata.ReportMetadataService;
import com.tcs.fincore.ReportBuilder.service.runtime.GlobalParamEngine;
import com.tcs.fincore.ReportBuilder.service.runtime.ReportRuntimeService;
import com.tcs.fincore.ReportBuilder.service.template.TemplateProcessorService;
import com.tcs.fincore.ReportBuilder.util.FileStorageUtil;
import com.tcs.fincore.ReportBuilder.util.ReportFileNameUtil;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Main orchestrator for batch report generation jobs
 * Coordinates pattern analysis, data materialization, and parallel report generation
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BatchJobOrchestrator {

    private final ObjectMapper objectMapper;
    private final ReportMetadataService metadataService;
    private final ReportRuntimeService runtimeService;
    private final TemplateProcessorService templateProcessor;
    private final QueryPatternAnalyzer patternAnalyzer;
    private final BatchDataMaterializer dataMaterializer;
    private final ResultDistributor resultDistributor;
    private final CalculationService calculationService;
    private final ExporterFactory exporterFactory;
    private final AggregatorService aggregatorService;
    private final FileSystem hdfsFileSystem;

    private final GlobalParamEngine globalParamEngine;
    private final GlobalParamBatchProcessor globalParamBatchProcessor;

    // Job tracking
    private final ConcurrentHashMap<String, BatchJobStatus> jobStatusMap = new ConcurrentHashMap<>();

    // Execution thread pool (separate from async executor)
    private final ExecutorService reportGenerationExecutor = new ThreadPoolExecutor(10,  // core threads
            20,  // max threads
            60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    @PostConstruct
    public void setupMapper() {
        // Ensure the mapper can handle Java 8 dates and doesn't crash on type drift
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        objectMapper.configure(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    /**
     * Submit batch job for processing
     */
    public BatchJobStatus submitJob(BatchJobRequest request) {

        // Generate job ID if not provided
        if (request.getJobId() == null || request.getJobId().isBlank()) {
            request.setJobId(generateJobId(request));
        }

        // Validate request
        validateRequest(request);

        // Create initial status
        BatchJobStatus status = createInitialStatus(request);
        jobStatusMap.put(request.getJobId(), status);

        // Start async processing
        processJobAsync(request);

        return status;
    }

    /**
     * Get job status
     */
    public BatchJobStatus getJobStatus(String jobId) {
        BatchJobStatus status = jobStatusMap.get(jobId);
        if (status == null) {
            throw new ValidationException("Job not found: " + jobId);
        }
        return status;
    }

    /**
     * Cancel running job
     */
    public void cancelJob(String jobId) {
        BatchJobStatus status = jobStatusMap.get(jobId);
        if (status == null) {
            throw new ValidationException("Job not found: " + jobId);
        }

        if (status.getStatus() == BatchJobStatus.JobState.RUNNING) {
            status.setStatus(BatchJobStatus.JobState.CANCELLED);
            status.setCurrentPhase(BatchJobStatus.JobPhase.CANCELLED);
            log.info("Job {} cancelled by user", jobId);
        }
    }

    /**
     * Main async processing method
     */
    @Async("reportExecutor")
    public void processJobAsync(BatchJobRequest request) {

        String jobId = request.getJobId();
        BatchJobStatus status = jobStatusMap.get(jobId);

        try {
            log.info("=== BATCH JOB STARTED: {} ===", jobId);
            log.info("Template: {}, Variant: {}, Reports: {}", request.getTemplateId(), request.getVariantCode(), request.getVaryingDimension().getValues().size());

            status.setStatus(BatchJobStatus.JobState.RUNNING);
            status.setStartTime(LocalDateTime.now());

            // PHASE 1: Analyze Template
            updatePhase(status, BatchJobStatus.JobPhase.ANALYZING_TEMPLATE);
            AnalysisResult analysis = analyzeTemplate(request);

//            log.info("Analysis :: {}", analysis.template);

            // PHASE 2: Materialize Data (if batch mode is beneficial)
            if (analysis.useBatchMode) {
                updatePhase(status, BatchJobStatus.JobPhase.MATERIALIZING_DATA);
                materializeData(analysis);
            } else {
                log.info("Batch mode not beneficial, using sequential processing");
            }

            // PHASE 3: Generate Reports
            updatePhase(status, BatchJobStatus.JobPhase.GENERATING_REPORTS);
            generateReportsInBatches(request, analysis, status);

            // PHASE 4: Finalization
            updatePhase(status, BatchJobStatus.JobPhase.COMPLETED);
            status.setStatus(BatchJobStatus.JobState.COMPLETED);
            status.setCompletionTime(LocalDateTime.now());

            log.info("=== BATCH JOB COMPLETED: {} ===", jobId);
            logJobSummary(status);

        } catch (Exception e) {
            log.error("Batch job {} failed: {}", jobId, e.getMessage(), e);
            updatePhase(status, BatchJobStatus.JobPhase.FAILED);
            status.setStatus(BatchJobStatus.JobState.FAILED);
            status.getErrors().setLastErrorMessage(e.getMessage());
            status.getErrors().setLastErrorTime(LocalDateTime.now());
        }

    }



//
//    private AnalysisResult analyzeTemplate(BatchJobRequest request) {
//
//        log.info("Phase 1: Analyzing template {}", request.getTemplateId());
//        long startTime = System.currentTimeMillis();
//
//        // 1. Load template and variant
//        ReportTemplate template = metadataService.getTemplate(request.getTemplateId());
//
//        // --- FIX START: Clone template for analysis to avoid polluting the master template ---
//        // runtimeService.applyBatchParams mutates the template (injects "0" placeholders).
//        // We must keep the original 'template' clean for Phase 3 (Generation).
//        ReportTemplate analysisTemplate = cloneTemplate(template);
//        // --- FIX END ---
//
//        ReportVariant variant = null;
//
//        if (request.getVariantCode() != null && !request.getVariantCode().isBlank()) {
//            variant = metadataService.getVariant(request.getTemplateId(), request.getVariantCode());
//        }
//
//        // 2. Resolve STATIC Global Params (Phase 1)
//        // We pass commonParams. The engine will resolve Dates, Constants, etc.
//        // It will SKIP params depending on the varying dimension (e.g., BRANCH_CODE) because strict=false.
//        Map<String, Object> staticContext = globalParamEngine.resolveContext(analysisTemplate, // Use analysisTemplate
//                request.getCommonParams(), false // strict=false: allow skipping dynamic dependencies
//        );
//        log.info("Static Global Context Resolved: {} params", staticContext.size());
//
//        // 3. Pre-fetch Dynamic Globals (Phase 1.5 - The N+1 Fix)
//        // This bulk-fetches params like BRANCH_NAME for all 25k branches in one go.
//        Map<String, Map<String, Object>> preFetchedGlobals = globalParamBatchProcessor.prefetchDynamicGlobals(analysisTemplate, // Use analysisTemplate
//                staticContext, request.getVaryingDimension().getParamName(), request.getVaryingDimension().getValues());
//        log.info("Batch Pre-fetch Complete: Loaded {} global parameters for {} dimension values :: {}", preFetchedGlobals.size(), request.getVaryingDimension().getValues().size(), preFetchedGlobals);
//// 4. Validate and prepare template structure
//        templateProcessor.validate(analysisTemplate); // Use analysisTemplate
//// Apply batch params logic (Legacy support for column filters, if needed)
//        if (variant != null) {
//            runtimeService.applyBatchParams(analysisTemplate, // CRITICAL: Mutate the copy, not the master
//                    variant, request.getCommonParams(),
//
//                    request.getVaryingDimension().getParamName());
//        }
//
//        templateProcessor.expandDynamicRows(analysisTemplate);
//        templateProcessor.assignIds(analysisTemplate);
//        // 5. Extract data specs for Main Report Body
//        List<DataSourceSpec> allSpecs = templateProcessor.extractDbSpecs(analysisTemplate);
//        // 6. Analyze patterns
//        List<QueryPattern> patterns = patternAnalyzer.analyzeTemplate(analysisTemplate, variant, request, allSpecs);
//        // 7. Decide on batch mode
//        boolean useBatchMode = patternAnalyzer.shouldUseBatchMode(request, patterns);
//        long elapsed = System.currentTimeMillis() - startTime;
//        log.info("Phase 1 complete in {}ms: Batch mode = {}", elapsed, useBatchMode);
//
////        // CRITICAL: Return the CLEAN 'template' in the result, so Phase 3 starts fresh
////        return new AnalysisResult(template, // <--- Return CLEAN template here
////                variant, allSpecs, patterns, useBatchMode, staticContext,
////                // Pass resolved static globals
////                preFetchedGlobals    // Pass bulk-fetched dynamic globals
////        );
//        // CRITICAL: Return the CLEAN 'template' in the result, so Phase 3 starts fresh
//        return new AnalysisResult(template, // <--- Return CLEAN template here
//                variant, allSpecs, patterns, useBatchMode, staticContext,
//                // Pass resolved static globals
//                preFetchedGlobals    // Pass bulk-fetched dynamic globals
//        );
//    }
//


private AnalysisResult analyzeTemplate(BatchJobRequest request) {

    log.info("Phase 1: Analyzing template {}", request.getTemplateId());
    long startTime = System.currentTimeMillis();

    // 1. Load Raw Template
    ReportTemplate rawTemplate = metadataService.getTemplate(request.getTemplateId());

    // 2. Prepare STRUCTURED Template (The "Golden Copy" for Generation)
    // We clone immediately and perform structural operations (Expansion + IDs).
    // This ensures the Template used for Generation has the EXACT same Cell IDs as the Analysis specs,
    // but WITHOUT the "0" placeholder pollution.
    ReportTemplate structuredTemplate = cloneTemplate(rawTemplate);

    // A. Resolve Static Globals (Dates/Constants) - needed for structural expansion (e.g., dynamic table names)
    Map<String, Object> staticContext = globalParamEngine.resolveContext(structuredTemplate,
            request.getCommonParams(), false);

    // B. Pre-fetch Dynamic Globals (N+1 Fix)
    Map<String, Map<String, Object>> preFetchedGlobals = globalParamBatchProcessor.prefetchDynamicGlobals(
            structuredTemplate, staticContext, request.getVaryingDimension().getParamName(),
            request.getVaryingDimension().getValues());

    // C. Perform Structural Transformations on the Golden Copy
    templateProcessor.validate(structuredTemplate);

    // This converts DYNAMIC rows to DATA rows and assigns stable IDs (cell_R1_C1)
    // CRITICAL: This must happen BEFORE we fork for analysis
    templateProcessor.expandDynamicRows(structuredTemplate);
    templateProcessor.assignIds(structuredTemplate);

    log.info("Structural preparation complete. Rows: {}", structuredTemplate.getReportData().getRows().size());

    // 3. Prepare ANALYSIS Template (The "Dirty Copy" for Spec Extraction)
    // We clone the structured template. We will inject "0" placeholders here to detect DB patterns.
    ReportTemplate analysisTemplate = cloneTemplate(structuredTemplate);

    ReportVariant variant = null;
    if (request.getVariantCode() != null && !request.getVariantCode().isBlank()) {
        variant = metadataService.getVariant(request.getTemplateId(), request.getVariantCode());
    }

    // Apply Batch Params to ANALYSIS template only.
    // This injects "0" and applies filters so we can see the WHERE clauses in extractDbSpecs.
    if (variant != null) {
        runtimeService.applyBatchParams(analysisTemplate,
                variant, request.getCommonParams(),
                request.getVaryingDimension().getParamName());
    }

    // 4. Extract data specs from ANALYSIS template
    // These specs now contain the Cell IDs from 'structuredTemplate' AND the Filters from 'analysisTemplate'
    List<DataSourceSpec> allSpecs = templateProcessor.extractDbSpecs(analysisTemplate);

    // 5. Analyze patterns
    List<QueryPattern> patterns = patternAnalyzer.analyzeTemplate(analysisTemplate, variant, request, allSpecs);

    // 6. Decide on batch mode
    boolean useBatchMode = patternAnalyzer.shouldUseBatchMode(request, patterns);

    long elapsed = System.currentTimeMillis() - startTime;
    log.info("Phase 1 complete in {}ms: Batch mode = {}", elapsed, useBatchMode);

    // 7. Return Result with the STRUCTURED TEMPLATE
    // Generation phase will clone this. It has IDs/Rows but NO dirty globals/filters.
    // 'generateSingleReport' calls 'prepareRuntimeContext', which will cleanly re-apply
    // Column Filters and Variant Rules using the REAL values for that iteration.
    return new AnalysisResult(
            structuredTemplate,  // <--- FIXED: Return the Clean, Expanded, ID'd template
            variant,
            allSpecs,
            patterns,
            useBatchMode,
            staticContext,
            preFetchedGlobals
    );
}


    /**
     * Phase 2: Materialize data in batch
     */
    private void materializeData(AnalysisResult analysis) {

        log.info("Phase 2: Materializing data for {} patterns", analysis.patterns.size());
        long startTime = System.currentTimeMillis();

//        log.info("analysis patterns :: {}", analysis.patterns);

        // Execute batch queries
        Map<String, Map<String, Map<String, Object>>> batchResults = dataMaterializer.materializeAll(analysis.patterns, 30); // 20 min timeout

        // Initialize result distributor
        resultDistributor.initialize(batchResults, analysis.patterns);

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Phase 2 complete in {}ms: {} patterns materialized", elapsed, batchResults.size());
    }

    /**
     * Phase 3: Generate reports in parallel batches
     */
    private void generateReportsInBatches(BatchJobRequest request, AnalysisResult analysis, BatchJobStatus status) {

        log.info("Phase 3: Generating {} reports in batches of {}", request.getVaryingDimension().getValues().size(), request.getBatchSize());

        List<String> allValues = request.getVaryingDimension().getValues();
        int batchSize = request.getBatchSize();
        int totalReports = allValues.size();

        status.getProgress().setTotalReports(totalReports);

        // Split into batches
        List<List<String>> batches = splitIntoBatches(allValues, batchSize);

        log.info("Split into {} batches", batches.size());

//        long startTime = System.currentTimeMillis();
        status.setBatchProcessStartTime(LocalDateTime.now());
        AtomicInteger completed = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);

        // Process batches in parallel
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int batchIndex = 0; batchIndex < batches.size(); batchIndex++) {
            List<String> batch = batches.get(batchIndex);
            final int batchNum = batchIndex + 1;

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {

                processBatch(request, analysis, batch, batchNum, status, completed, failed);

            }, reportGenerationExecutor);

            futures.add(future);
        }

        // Wait for all batches
        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allOf.get(); // Wait indefinitely (individual reports have timeouts)
        } catch (Exception e) {
            log.error("Batch processing interrupted: {}", e.getMessage());
        }

//        long elapsed  = Duration.between(status.getBatchProcessStartTime(), LocalDateTime.now()).getSeconds();
//        double reportsPerMinute = (completed.get() / (elapsed / 60000.0));
//
//        status.getProgress().setReportsPerMinute((long) reportsPerMinute);
        status.getProgress().setCompletedReports(completed.get());
        status.getProgress().setFailedReports(failed.get());

//        log.info("Phase 3 complete in {}ms: {} reports, {} reports/min",
//                elapsed, completed.get(), reportsPerMinute);
        log.info("Phase 3 complete");
    }

    /**
     * Process a single batch of reports
     */
    private void processBatch(BatchJobRequest request, AnalysisResult analysis, List<String> dimensionValues, int batchNum, BatchJobStatus status, AtomicInteger completedCounter, AtomicInteger failedCounter) {

        log.info("Batch {}: Processing {} reports", batchNum, dimensionValues.size());

        for (String dimensionValue : dimensionValues) {

            // Check if job was cancelled
            if (status.getStatus() == BatchJobStatus.JobState.CANCELLED) {
                log.info("Batch {} cancelled", batchNum);
                return;
            }

            try {
                generateSingleReport(request, analysis, dimensionValue);

                completedCounter.incrementAndGet();
                status.getProgress().getCompletedValues().add(dimensionValue);

                // Update progress periodically
                if (completedCounter.get() % 100 == 0) {
                    updateProgress(status, completedCounter.get());
                }

            } catch (Exception e) {
                log.error("Failed to generate report for {}: {}", dimensionValue, e.getMessage());

                failedCounter.incrementAndGet();
                status.getProgress().getFailedValues().add(dimensionValue);

                // Record error
                BatchJobStatus.ErrorInfo.ErrorDetail error = BatchJobStatus.ErrorInfo.ErrorDetail.builder().dimensionValue(dimensionValue).errorMessage(e.getMessage()).errorType(e.getClass().getSimpleName()).timestamp(LocalDateTime.now()).build();

                synchronized (status.getErrors()) {
                    status.getErrors().getRecentErrors().add(error);
                    status.getErrors().setTotalErrors(status.getErrors().getTotalErrors() + 1);

                    // Keep only last 100 errors
                    if (status.getErrors().getRecentErrors().size() > 100) {
                        status.getErrors().getRecentErrors().remove(0);
                    }
                }
            }
        }

        log.info("Batch {} complete: {} reports", batchNum, dimensionValues.size());
    }


    /**
     * Generate a single report for specific dimension value.
     * NOW ENHANCED: Injects pre-fetched globals to avoid DB hits per report.
     */
    private void generateSingleReport(BatchJobRequest request, AnalysisResult analysis, String dimensionValue) throws Exception {

        // 1. Prepare Inputs for this specific run
        // Start with the already-resolved static context (Dates, Math, etc.)
        Map<String, Object> runInputs = new HashMap<>(analysis.staticContext);

        // Add the specific dimension value (e.g., BRANCH_CODE = "05999")
        runInputs.put(request.getVaryingDimension().getParamName(), dimensionValue);

        // 2. Inject Pre-fetched Globals (Solving N+1)
        // If we pre-fetched BRANCH_NAME for "05999", inject it now.
        if (analysis.preFetchedGlobals != null) {
            analysis.preFetchedGlobals.forEach((paramName, valueMap) -> {
                if (valueMap.containsKey(dimensionValue)) {
                    Object val = valueMap.get(dimensionValue);
                    runInputs.put(paramName, val);
                    // log.trace("Injected batch global '{}' = '{}'", paramName, val);
                }
            });
        }

        ReportViewRequest viewRequest = new ReportViewRequest();
        viewRequest.setTemplateId(request.getTemplateId());
        viewRequest.setVariantCode(request.getVariantCode());
        viewRequest.setParams(runInputs);

        // 3. Clone template (to avoid concurrent modification of the shared Analysis template)
        ReportTemplate templateCopy = cloneTemplate(analysis.template);

//        log.info("TemplateCopy :: {}", templateCopy);

        // 4. Prepare Runtime Context
        // This is now highly optimized: The GlobalParamEngine inside will see that
        // BRANCH_NAME, REPORT_DATE etc. are ALREADY in runInputs, so it skips the DB queries.
        runtimeService.prepareRuntimeContext(templateCopy, analysis.variant, runInputs);

        // 5. Get data (from batch or individual queries) - Standard Flow
        Map<String, Object> dataResults;
        if (analysis.useBatchMode) {
            // Get from distributed batch results
            dataResults = resultDistributor.getResultsForDimension(dimensionValue, analysis.allSpecs);

            // Handle fallback specs (query individually if needed)
            List<DataSourceSpec> fallbackSpecs = new ArrayList<>();
            for (Map.Entry<String, Object> entry : dataResults.entrySet()) {
                if ("FALLBACK".equals(entry.getValue())) {
                    analysis.allSpecs.stream().filter(s -> s.getSpecId().equals(entry.getKey())).findFirst().ifPresent(fallbackSpecs::add);
                }
            }

            if (!fallbackSpecs.isEmpty()) {
                Map<String, Object> fallbackResults = aggregatorService.fetchAggregates(fallbackSpecs);
                dataResults.putAll(fallbackResults);
            }

        } else {
            // Sequential mode: fetch all data
            dataResults = aggregatorService.fetchAggregates(analysis.allSpecs);
        }

        // 6. Calculate formulas
        Map<String, Object> computed = calculationService.calculate(templateCopy, dataResults);

        // 7. Generate export model
        ExportModel exportModel = templateProcessor.toExportModel(templateCopy, computed);

        // 8. Generate and save reports
        saveReportsToHdfs(request, analysis, dimensionValue, exportModel);
    }

    /**
     * Save generated reports to HDFS
     */
    private void saveReportsToHdfs(BatchJobRequest request, AnalysisResult analysis, String dimensionValue, ExportModel exportModel) throws Exception {

        // Prepare file name
        Map<String, Object> params = new HashMap<>(request.getCommonParams());
        params.put(request.getVaryingDimension().getParamName(), dimensionValue);

        String fileName = ReportFileNameUtil.buildFileName(analysis.template, analysis.variant, params);

        // Determine HDFS directory
        String basePath = request.getHdfsBasePath() != null ? request.getHdfsBasePath() : "/reports";

        Path directory = createHdfsDirectory(basePath, LocalDate.now(), analysis.template.getReportMeta().getReportId() + "/" + dimensionValue);

        // Generate and save each format
        List<String> formats = request.getOutputFormats() != null ? request.getOutputFormats() : List.of("pdf", "xlsx");

        for (String format : formats) {
            try {
                ReportExporter exporter = exporterFactory.getExporter(format);
                byte[] content = exporter.generate(exportModel);

                Path filePath = new Path(directory, fileName + exporter.getFileExtension());

//                FileStorageUtil.writeToFile(content, hdfsFileSystem, filePath);
                String testPath = "./batch-test/" + fileName + exporter.getFileExtension();

                // Ensure directory exists
                java.io.File testDir = new java.io.File("./batch-test");
                if (!testDir.exists()) {
                    testDir.mkdirs();
                }

                try (FileOutputStream fos = new FileOutputStream(testPath)) {
                    fos.write(content);
                    log.info("Successfully saved report to: {}", testPath);
                } catch (IOException e) {
                    log.error("Failed to save report to {}: {}", testPath, e.getMessage());
                }
            } catch (Exception e) {
                log.error("Failed to save {} format for {}: {}", format, dimensionValue, e.getMessage());
                // Continue with other formats
            }
        }
    }

    // ========== HELPER METHODS ==========

    private void validateRequest(BatchJobRequest request) {
        if (request.getTemplateId() == null || request.getTemplateId().isBlank()) {
            throw new ValidationException("templateId is required");
        }

        if (request.getVariantCode() == null || request.getVariantCode().isBlank()) {
            throw new ValidationException("variantCode is required");
        }


        if (request.getVaryingDimension() == null) {
            throw new ValidationException("varyingDimension is required");
        }

        if (request.getVaryingDimension().getValues() == null || request.getVaryingDimension().getValues().isEmpty()) {
            throw new ValidationException("varyingDimension values cannot be empty");
        }

        if (request.getCommonParams() == null) {
            request.setCommonParams(new HashMap<>());
        }
    }

    private String generateJobId(BatchJobRequest request) {
        return String.format("%s_%s_%s", request.getTemplateId(), request.getVaryingDimension().getParamName(), LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")));
    }

    private BatchJobStatus createInitialStatus(BatchJobRequest request) {
        return BatchJobStatus.builder().jobId(request.getJobId()).templateId(request.getTemplateId()).variantCode(request.getVariantCode()).currentPhase(BatchJobStatus.JobPhase.SUBMITTED).status(BatchJobStatus.JobState.PENDING).progress(BatchJobStatus.ProgressInfo.builder().totalReports(request.getVaryingDimension().getValues().size()).completedReports(0).failedReports(0).percentageComplete(0.0).completedValues(ConcurrentHashMap.newKeySet()).failedValues(ConcurrentHashMap.newKeySet()).build()).errors(BatchJobStatus.ErrorInfo.builder().totalErrors(0).recentErrors(new ArrayList<>()).build()).build();
    }

    private void updatePhase(BatchJobStatus status, BatchJobStatus.JobPhase phase) {
        status.setCurrentPhase(phase);
        status.setLastUpdateTime(LocalDateTime.now());
        log.info("Job {} entering phase: {}", status.getJobId(), phase);
    }

    private void updateProgress(BatchJobStatus status, int completed) {
        status.getProgress().setCompletedReports(completed);
        status.getProgress().setPercentageComplete((completed * 100.0) / status.getProgress().getTotalReports());

        if (completed > 0 && status.getBatchProcessStartTime() != null) {
            long elapsedSeconds = Duration.between(status.getBatchProcessStartTime(), LocalDateTime.now()).getSeconds();

            if (elapsedSeconds > 0) {
                //(Rate) Reports per minute
                double reportsPerSecond = (double) completed / elapsedSeconds;
                status.getProgress().setReportsPerMinute((long) (reportsPerSecond * 60));

                //Average Time per Report (ms)
                status.getProgress().setAverageReportTimeMs((long) (elapsedSeconds / 1000.0) / completed);

                //ETA (Estimated Time Remaining in Seconds)
                int remainingReports = status.getProgress().getTotalReports() - completed;
                if (reportsPerSecond > 0) {
                    status.setEstimatedTimeRemainingSeconds((long) (remainingReports / reportsPerSecond));
                }
            }
        }

        status.setLastUpdateTime(LocalDateTime.now());
    }

    private List<List<String>> splitIntoBatches(List<String> items, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < items.size(); i += batchSize) {
            batches.add(items.subList(i, Math.min(items.size(), i + batchSize)));
        }
        return batches;
    }

    private Path createHdfsDirectory(String basePath, LocalDate date, String afterDateFilePath) throws Exception {
        String dateStr = date.format(DateTimeFormatter.ISO_LOCAL_DATE);
        Path path = new Path(basePath, dateStr + "/" + afterDateFilePath);

        if (!hdfsFileSystem.exists(path)) {
            hdfsFileSystem.mkdirs(path);
        }

        return path;
    }

//    private ReportTemplate cloneTemplate(ReportTemplate template) {
//        try {
//            String json = objectMapper.writeValueAsString(template);
//            return objectMapper.readValue(json, ReportTemplate.class);
//        } catch (JsonProcessingException e) {
//            log.error("Error cloning template :: {}", e.getMessage());
//            throw new RuntimeException("Failed to clone template", e);
//        }
//
//    }

    private ReportTemplate cloneTemplate(ReportTemplate template) {
        try {
            // High-performance deep copy using optimized ObjectMapper
            return objectMapper.readValue(objectMapper.writeValueAsBytes(template), // byte[] is faster than String for cloning
                    ReportTemplate.class);
        } catch (IOException e) {
            log.error("Deep copy failed for template {}: {}", template.getReportMeta().getReportId(), e.getMessage());
            throw new RuntimeException("Critical: Template cloning failed", e);
        }
    }

    private void logJobSummary(BatchJobStatus status) {
        log.info("===== JOB SUMMARY =====");
        log.info("Job ID: {}", status.getJobId());
        log.info("Total Reports: {}", status.getProgress().getTotalReports());
        log.info("Completed: {}", status.getProgress().getCompletedReports());
        log.info("Failed: {}", status.getProgress().getFailedReports());
        log.info("Success Rate: {}%", (status.getProgress().getCompletedReports() * 100.0) / status.getProgress().getTotalReports());
        log.info("========================");
    }

    /**
     * Inner class for analysis results.
     * Updated to hold context state.
     */
    private static class AnalysisResult {
        final ReportTemplate template;
        final ReportVariant variant;
        final List<DataSourceSpec> allSpecs;
        final List<QueryPattern> patterns;
        final boolean useBatchMode;

        // NEW fields for Context Hydration
        final Map<String, Object> staticContext;
        final Map<String, Map<String, Object>> preFetchedGlobals;

        AnalysisResult(ReportTemplate template, ReportVariant variant, List<DataSourceSpec> allSpecs, List<QueryPattern> patterns, boolean useBatchMode, Map<String, Object> staticContext, Map<String, Map<String, Object>> preFetchedGlobals) {
            this.template = template;
            this.variant = variant;
            this.allSpecs = allSpecs;
            this.patterns = patterns;
            this.useBatchMode = useBatchMode;
            this.staticContext = staticContext;
            this.preFetchedGlobals = preFetchedGlobals;
        }
    }
}

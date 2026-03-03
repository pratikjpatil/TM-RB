






package com.tcs.fincore.ReportBuilder.batch.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.batch.analyzer.QueryPatternAnalyzer;
import com.tcs.fincore.ReportBuilder.batch.distributor.ResultDistributor;
import com.tcs.fincore.ReportBuilder.batch.materializer.BatchDataMaterializer;
import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.batch.processor.GlobalParamBatchProcessor;
import com.tcs.fincore.ReportBuilder.batch.status.BatchJobStatusStore;
import com.tcs.fincore.ReportBuilder.batch.store.BatchResultStore;
import com.tcs.fincore.ReportBuilder.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.dto.BatchJobStatus;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;
import com.tcs.fincore.ReportBuilder.kafka.publisher.ReportEventPublisher;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.ReportViewRequest;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
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
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    private final BatchJobStatusStore batchJobStatusStore;
    private final BatchResultStore batchResultStore;
    private final CalculationService calculationService;
    private final ExporterFactory exporterFactory;
    private final AggregatorService aggregatorService;
    private final FileSystem hdfsFileSystem;
    private final GlobalParamEngine globalParamEngine;
    private final GlobalParamBatchProcessor globalParamBatchProcessor;


    private final ReportEventPublisher eventPublisher;

    @Value("${report.batch.materialization.timeout-minutes:30}")
    private long materializationTimeoutMinutes;

    @Value("${report.batch.generation.timeout-minutes:240}")
    private long generationTimeoutMinutes;

    @Value("${report.batch.status.ttl-minutes:120}")
    private long statusTtlMinutes;

    @Value("${report.batch.materialization.max-inmemory-estimated-rows:2000000}")
    private long maxInMemoryEstimatedRows;

    @Value("${report.batch.result-store:memory}")
    private String resultStoreType;

    @Value("${report.batch.hdfs-base-path:/reports}")
    private String defaultHdfsBasePath;


    private final ConcurrentHashMap<String, AtomicBoolean> cancellationTokens = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<Future<?>>> jobFutures = new ConcurrentHashMap<>();

    /**
     * Per-job execution context store.
     * Needed so the async thread can access the context that was originally set by the caller.
     */
    private final ConcurrentHashMap<String, ReportExecutionContext> executionContexts =
            new ConcurrentHashMap<>();

    private ExecutorService reportGenerationExecutor;

    // ----- Lifecycle -----

    @PostConstruct
    public void setup() {
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        objectMapper.configure(
                com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        reportGenerationExecutor = new ThreadPoolExecutor(
                10, 20,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        log.info("BatchJobOrchestrator initialized. materializationTimeout={}m, generationTimeout={}m",
                materializationTimeoutMinutes, generationTimeoutMinutes);
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down reportGenerationExecutor...");
        reportGenerationExecutor.shutdown();
        try {
            if (!reportGenerationExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate in 30s — forcing shutdown");
                reportGenerationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            reportGenerationExecutor.shutdownNow();
        }
    }

    @Scheduled(fixedDelayString = "${report.batch.status.cleanup-interval-ms:1800000}")
    public void evictStaleJobStatuses() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(statusTtlMinutes);
        int evicted = batchJobStatusStore.evictStaleTerminal(cutoff);
        if (evicted > 0) {
            log.info("Evicted {} stale job status entries (TTL={}m)", evicted, statusTtlMinutes);
        }
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Submit a batch job triggered via REST API.
     * No Kafka lifecycle events will be published for these jobs.
     *
     * <p>This is the existing contract — unchanged for backward compatibility.
     */
    public BatchJobStatus submitJob(BatchJobRequest request) {
        return submitJob(request, ReportExecutionContext.fromRest());
    }

    /**
     * Submit a batch job with an explicit execution context.
     *
     * <p>Used by the Kafka consumer to pass upstream correlation IDs and mark
     * the job as Kafka-triggered (enabling lifecycle event publishing).
     *
     * @param request  the batch job specification
     * @param context  the execution context (trigger source + correlation IDs)
     */
    public BatchJobStatus submitJob(BatchJobRequest request, ReportExecutionContext context) {
        // If IDs provided via context (Kafka), use them as the jobId
        if (context.isKafkaTriggered()
                && request.getJobId() == null || (request.getJobId() != null && request.getJobId().isBlank())) {
            // The Kafka consumer already set the composite ID — trust it.
            // If for some reason it wasn't set, derive it here.
            if (request.getJobId() == null || request.getJobId().isBlank()) {
                request.setJobId(generateJobId(request));
            }
        } else if (request.getJobId() == null || request.getJobId().isBlank()) {
            request.setJobId(generateJobId(request));
        }

        validateRequest(request);

        BatchJobStatus status = createInitialStatus(request);
        persistStatus(status);

        cancellationTokens.put(request.getJobId(), new AtomicBoolean(false));
        jobFutures.put(request.getJobId(), Collections.synchronizedList(new ArrayList<>()));
        executionContexts.put(request.getJobId(), context);

        processJobAsync(request);
        return status;
    }

    public BatchJobStatus getJobStatus(String jobId) {
        BatchJobStatus status = batchJobStatusStore.find(jobId).orElse(null);
        if (status == null) throw new ValidationException("Job not found: " + jobId);
        return status;
    }

    public void cancelJob(String jobId) {
        BatchJobStatus status = batchJobStatusStore.find(jobId).orElse(null);
        if (status == null) throw new ValidationException("Job not found: " + jobId);

        if (status.getStatus() == BatchJobStatus.JobState.RUNNING) {
            AtomicBoolean token = cancellationTokens.get(jobId);
            if (token != null) token.set(true);

            List<Future<?>> futures = jobFutures.get(jobId);
            if (futures != null) {
                futures.forEach(f -> f.cancel(true));
                log.info("Cancelled {} futures for job {}", futures.size(), jobId);
            }

            status.setStatus(BatchJobStatus.JobState.CANCELLED);
            status.setCurrentPhase(BatchJobStatus.JobPhase.CANCELLED);
            status.setCompletionTime(LocalDateTime.now());
            persistStatus(status);
            log.info("Job {} cancelled", jobId);
        }
    }

    // -------------------------------------------------------------------------
    // Async processing
    // -------------------------------------------------------------------------

    @Async("reportExecutor")
    public void processJobAsync(BatchJobRequest request) {
        String jobId = request.getJobId();
        BatchJobStatus status = batchJobStatusStore.find(jobId)
                .orElseThrow(() -> new ValidationException("Job not found: " + jobId));
        AtomicBoolean cancelled = cancellationTokens.get(jobId);
        ReportExecutionContext context = executionContexts.getOrDefault(
                jobId, ReportExecutionContext.fromRest());

        // Track start instant for event duration calculation
        Instant jobStartInstant = Instant.now();

        try {
            log.info("=== BATCH JOB STARTED: {} (trigger={}) ===", jobId, context.getTriggerSource());
            status.setStatus(BatchJobStatus.JobState.RUNNING);
            status.setStartTime(LocalDateTime.now());
            persistStatus(status);

            // -- Publish STARTED event (Kafka-triggered jobs only) ---------------
            eventPublisher.publishBatchStarted(context, status);

            updatePhase(status, BatchJobStatus.JobPhase.ANALYZING_TEMPLATE);
            AnalysisResult analysis = analyzeTemplate(request);

            if (cancelled.get()) { markCancelled(status, jobId); return; }

            if (analysis.useBatchMode) {
                updatePhase(status, BatchJobStatus.JobPhase.MATERIALIZING_DATA);
                materializeData(jobId, analysis);
            } else {
                log.info("Batch mode not beneficial — using sequential per-report queries");
            }

            if (cancelled.get()) { markCancelled(status, jobId); return; }

            updatePhase(status, BatchJobStatus.JobPhase.GENERATING_REPORTS);
            generateReportsInBatches(request, analysis, status, cancelled);

            if (!cancelled.get()) {
                updatePhase(status, BatchJobStatus.JobPhase.COMPLETED);
                status.setStatus(BatchJobStatus.JobState.COMPLETED);
                status.setCompletionTime(LocalDateTime.now());
                persistStatus(status);
                log.info("=== BATCH JOB COMPLETED: {} ===", jobId);
                logJobSummary(status);

                // -- Publish COMPLETED event (Kafka-triggered jobs only) ----------
                eventPublisher.publishBatchTerminal(context, status, jobStartInstant);
            }

        } catch (RuntimeException e) {
            log.error("Batch job {} failed: {}", jobId, e.getMessage(), e);
            status.setCurrentPhase(BatchJobStatus.JobPhase.FAILED);
            status.setStatus(BatchJobStatus.JobState.FAILED);
            status.setCompletionTime(LocalDateTime.now());
            status.getErrors().setLastErrorMessage(e.getMessage());
            status.getErrors().setLastErrorTime(LocalDateTime.now());
            persistStatus(status);

            // -- Publish FAILED event (Kafka-triggered jobs only) -----------------
            eventPublisher.publishBatchTerminal(context, status, jobStartInstant);

        } finally {
            batchResultStore.cleanupJob(jobId);
            resultDistributor.clearJobContext(jobId);
            cancellationTokens.remove(jobId);
            jobFutures.remove(jobId);
            executionContexts.remove(jobId);
        }
    }

    // -------------------------------------------------------------------------
    // Phase 1: Template Analysis (unchanged)
    // -------------------------------------------------------------------------

    private AnalysisResult analyzeTemplate(BatchJobRequest request) {
        log.info("Phase 1: Analyzing template {}", request.getTemplateId());
        long startTime = System.currentTimeMillis();

        ReportTemplate rawTemplate = metadataService.getTemplate(request.getTemplateId());
        ReportTemplate structuredTemplate = cloneTemplate(rawTemplate);

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

        ReportTemplate analysisTemplate = cloneTemplate(structuredTemplate);
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

        return new AnalysisResult(structuredTemplate, variant, allSpecs,
                patterns, useBatchMode, staticContext, preFetchedGlobals);
    }

    // -------------------------------------------------------------------------
    // Phase 2: Data Materialization (unchanged)
    // -------------------------------------------------------------------------

    private void materializeData(String jobId, AnalysisResult analysis) {
        log.info("Phase 2: Materializing {} patterns (timeout={}m)",
                analysis.patterns.size(), materializationTimeoutMinutes);
        long startTime = System.currentTimeMillis();
        BatchJobStatus status = batchJobStatusStore.find(jobId).orElse(null);

        long estimatedRows = analysis.patterns.stream().mapToLong(QueryPattern::estimateResultSetSize).sum();
        if (status != null) {
            status.getProgress().setEstimatedMaterializationRows(estimatedRows);
            status.getProgress().setMaterializedRows(0L);
            status.setLastUpdateTime(LocalDateTime.now());
            persistStatus(status);
        }

        if ("memory".equalsIgnoreCase(resultStoreType) && estimatedRows > maxInMemoryEstimatedRows) {
            throw new ValidationException("Estimated materialized rows " + estimatedRows
                    + " exceed in-memory threshold " + maxInMemoryEstimatedRows
                    + ". Switch to Redis result store for large batch workloads.");
        }

        batchResultStore.initializeJob(jobId, Duration.ofMinutes(statusTtlMinutes));
        AtomicLong pendingRows = new AtomicLong(0);
        dataMaterializer.materializeAll(jobId, analysis.patterns, materializationTimeoutMinutes, rows -> {
            if (status == null) {
                return;
            }
            long buffered = pendingRows.addAndGet(rows);
            if (buffered >= 10_000) {
                synchronized (status.getProgress()) {
                    long total = status.getProgress().getMaterializedRows() + pendingRows.getAndSet(0);
                    status.getProgress().setMaterializedRows(total);
                }
                status.setLastUpdateTime(LocalDateTime.now());
                persistStatus(status);
            }
        });

        if (status != null) {
            long remainder = pendingRows.getAndSet(0);
            if (remainder > 0) {
                synchronized (status.getProgress()) {
                    status.getProgress().setMaterializedRows(
                            status.getProgress().getMaterializedRows() + remainder);
                }
            }
            status.setLastUpdateTime(LocalDateTime.now());
            persistStatus(status);
        }

        resultDistributor.initialize(
                jobId, analysis.patterns,
                analysis.preFetchedGlobals, analysis.template);

        log.info("Phase 2 complete in {}ms: {} patterns materialized",
                System.currentTimeMillis() - startTime, analysis.patterns.size());
    }

    // -------------------------------------------------------------------------
    // Phase 3: Report Generation (unchanged)
    // -------------------------------------------------------------------------

    private void generateReportsInBatches(BatchJobRequest request, AnalysisResult analysis,
                                          BatchJobStatus status, AtomicBoolean cancelled) {
        List<String> allValues = request.getVaryingDimension().getValues();
        int totalReports = allValues.size();
        status.getProgress().setTotalReports(totalReports);
        status.setBatchProcessStartTime(LocalDateTime.now());

        List<List<String>> batches = splitIntoBatches(allValues, request.getBatchSize());
        log.info("Phase 3: Generating {} reports in {} batches", totalReports, batches.size());

        AtomicInteger completedCount = new AtomicInteger(0);

        for (List<String> batch : batches) {
            if (cancelled.get()) {
                markCancelled(status, status.getJobId());
                return;
            }

            List<CompletableFuture<Void>> batchFutures = new ArrayList<>();

            for (String dimensionValue : batch) {
                if (cancelled.get()) break;

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        generateSingleReport(request, analysis, dimensionValue);
                        int done = completedCount.incrementAndGet();
                        status.getProgress().getCompletedValues().add(dimensionValue);
                        updateProgress(status, done);
                    } catch (RuntimeException e) {
                        log.error("Failed to generate report for {}: {}", dimensionValue, e.getMessage(), e);
                        status.getProgress().getFailedValues().add(dimensionValue);
                        status.getProgress().setFailedReports(
                                status.getProgress().getFailedReports() + 1);
                        recordError(status, dimensionValue, e);
                        persistStatus(status);
                    }
                }, reportGenerationExecutor);

                batchFutures.add(future);
                jobFutures.get(status.getJobId()).add(future);
            }

            try {
                CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]))
                        .get(generationTimeoutMinutes, TimeUnit.MINUTES);
            } catch (TimeoutException e) {
                log.error("Batch timed out after {}m for job {}", generationTimeoutMinutes, status.getJobId());
                batchFutures.forEach(f -> f.cancel(true));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                batchFutures.forEach(f -> f.cancel(true));
            } catch (ExecutionException e) {
                log.error("Batch execution error for job {}: {}", status.getJobId(), e.getMessage());
            }
        }

        // Determine final state
        int failed = status.getProgress().getFailedReports();
        int completed = completedCount.get();
        if (failed > 0 && completed > 0) {
            status.setStatus(BatchJobStatus.JobState.PARTIALLY_COMPLETED);
            persistStatus(status);
        }
    }

    private void generateSingleReport(BatchJobRequest request, AnalysisResult analysis,
                                      String dimensionValue) throws Exception {
        Map<String, Object> runInputs = new HashMap<>(analysis.staticContext);
        runInputs.put(request.getVaryingDimension().getParamName(), dimensionValue);

        if (analysis.preFetchedGlobals != null) {
            analysis.preFetchedGlobals.forEach((paramName, valueMap) -> {
                if (valueMap.containsKey(dimensionValue)) {
                    runInputs.put(paramName, valueMap.get(dimensionValue));
                }
            });
        }

        ReportTemplate templateCopy = cloneTemplate(analysis.template);
        runtimeService.prepareRuntimeContext(templateCopy, analysis.variant, runInputs);
        List<DataSourceSpec> currentSpecs = templateProcessor.extractDbSpecs(templateCopy);

        Map<String, Object> dataResults;
        if (analysis.useBatchMode) {
            dataResults = resultDistributor.getResultsForDimension(request.getJobId(), dimensionValue, currentSpecs);
            List<DataSourceSpec> fallbackSpecs = new ArrayList<>();
            for (DataSourceSpec spec : currentSpecs) {
                if ("FALLBACK".equals(dataResults.get(spec.getSpecId()))) {
                    fallbackSpecs.add(spec);
                }
            }
            if (!fallbackSpecs.isEmpty()) {
                dataResults.putAll(aggregatorService.fetchAggregates(fallbackSpecs));
            }
        } else {
            dataResults = aggregatorService.fetchAggregates(currentSpecs);
        }

        Map<String, Object> computed = calculationService.calculate(templateCopy, dataResults);
        ExportModel exportModel = templateProcessor.toExportModel(templateCopy, computed);
        saveReportsToHdfs(request, analysis, dimensionValue, exportModel);
    }

    private String sanitizeDimensionValueForPath(String dimensionValue) {
        if (dimensionValue == null || dimensionValue.isBlank()) {
            throw new ValidationException("Dimension value cannot be null or blank");
        }
        String sanitized = dimensionValue.replaceAll("[^A-Za-z0-9_\\-]", "_");
        if (sanitized.contains("..") || sanitized.startsWith("/") || sanitized.startsWith("\\")) {
            throw new ValidationException("Invalid dimension value for path: " + dimensionValue);
        }
        if (sanitized.length() > 64) {
            sanitized = sanitized.substring(0, 64);
        }
        return sanitized;
    }

    private void saveReportsToHdfs(BatchJobRequest request, AnalysisResult analysis,
                                   String dimensionValue, ExportModel exportModel) throws Exception {
        String safeDimensionValue = sanitizeDimensionValueForPath(dimensionValue);
        String safeReportId = sanitizeDimensionValueForPath(
                analysis.template.getReportMeta().getReportId());

        Map<String, Object> params = new HashMap<>(request.getCommonParams());
        params.put(request.getVaryingDimension().getParamName(), dimensionValue);

        String fileName = ReportFileNameUtil.buildFileName(analysis.template, analysis.variant, params);

        String basePath = request.getHdfsBasePath() != null
                ? request.getHdfsBasePath() : defaultHdfsBasePath;

        Path directory = createHdfsDirectory(
                basePath, LocalDate.now(), safeReportId + "/" + safeDimensionValue);

        List<String> formats = request.getOutputFormats() != null
                ? request.getOutputFormats() : List.of("pdf", "xlsx");

        List<String> allowedFormats = List.of("pdf", "xlsx", "csv", "psv");
        for (String format : formats) {
            if (!allowedFormats.contains(format.toLowerCase())) {
                log.warn("Unsupported output format requested: {}. Skipping.", format);
                continue;
            }
            try {
                ReportExporter exporter = exporterFactory.getExporter(format);
                byte[] content = exporter.generate(exportModel);
                Path filePath = new Path(directory, fileName + exporter.getFileExtension());
                FileStorageUtil.writeToFile(content, hdfsFileSystem, filePath);
                log.debug("Saved {} report for {}: {}", format, safeDimensionValue, filePath);
            } catch (RuntimeException e) {
                log.error("Failed to save {} report for {}: {}", format, safeDimensionValue, e.getClass().getSimpleName());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Progress tracking
    // -------------------------------------------------------------------------

    private void updateProgress(BatchJobStatus status, int completed) {
        status.getProgress().setCompletedReports(completed);
        int total = status.getProgress().getTotalReports();
        if (total > 0) {
            status.getProgress().setPercentageComplete((completed * 100.0) / total);
        }

        if (completed > 0 && status.getBatchProcessStartTime() != null) {
            long elapsedSeconds = Duration.between(
                    status.getBatchProcessStartTime(), LocalDateTime.now()).getSeconds();
            if (elapsedSeconds > 0) {
                double reportsPerSecond = (double) completed / elapsedSeconds;
                status.getProgress().setReportsPerMinute((long) (reportsPerSecond * 60));
                status.getProgress().setAverageReportTimeMs((elapsedSeconds * 1000L) / completed);
                int remaining = total - completed;
                if (reportsPerSecond > 0) {
                    status.setEstimatedTimeRemainingSeconds((long) (remaining / reportsPerSecond));
                }
            }
        }
        status.setLastUpdateTime(LocalDateTime.now());
        persistStatus(status);
    }

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

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
        if (request.getVaryingDimension().getValues() == null
                || request.getVaryingDimension().getValues().isEmpty()) {
            throw new ValidationException("varyingDimension.values cannot be empty");
        }
        if (request.getCommonParams() == null) {
            request.setCommonParams(new HashMap<>());
        }

        int batchSize = request.getBatchSize();
        if (batchSize <= 0) {
            log.warn("Invalid batchSize={} — defaulting to 100", batchSize);
            request.setBatchSize(100);
        }

        int maxReports = request.getVaryingDimension().getValues().size();
        if (maxReports > 50_000) {
            throw new ValidationException(
                    "Too many dimension values (" + maxReports + ", max: 50000)");
        }
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    private List<List<String>> splitIntoBatches(List<String> items, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < items.size(); i += batchSize) {
            batches.add(new ArrayList<>(items.subList(i, Math.min(items.size(), i + batchSize))));
        }
        return batches;
    }

    private void recordError(BatchJobStatus status, String dimensionValue, Exception e) {
        BatchJobStatus.ErrorInfo.ErrorDetail detail =
                BatchJobStatus.ErrorInfo.ErrorDetail.builder()
                        .dimensionValue(dimensionValue)
                        .errorMessage(e.getMessage())
                        .errorType(e.getClass().getSimpleName())
                        .timestamp(LocalDateTime.now())
                        .build();

        BatchJobStatus.ErrorInfo errors = status.getErrors();
        synchronized (errors) {
            errors.setTotalErrors(errors.getTotalErrors() + 1);
            errors.setLastErrorMessage(e.getMessage());
            errors.setLastErrorTime(LocalDateTime.now());

            List<BatchJobStatus.ErrorInfo.ErrorDetail> recents = errors.getRecentErrors();
            if (!(recents instanceof LinkedList)) {
                recents = new LinkedList<>(recents);
                errors.setRecentErrors(recents);
            }
            ((LinkedList<BatchJobStatus.ErrorInfo.ErrorDetail>) recents).addFirst(detail);
            if (recents.size() > 100) {
                ((LinkedList<BatchJobStatus.ErrorInfo.ErrorDetail>) recents).removeLast();
            }
        }
    }

    private void markCancelled(BatchJobStatus status, String jobId) {
        status.setStatus(BatchJobStatus.JobState.CANCELLED);
        status.setCurrentPhase(BatchJobStatus.JobPhase.CANCELLED);
        status.setCompletionTime(LocalDateTime.now());

        ReportExecutionContext context = executionContexts.getOrDefault(jobId, ReportExecutionContext.fromRest());
        eventPublisher.publishBatchTerminal(context, status,
                status.getStartTime() != null
                        ? status.getStartTime().toInstant(ZoneOffset.UTC) : Instant.now());

        log.info("Job {} stopped — cancelled flag was set", jobId);
        persistStatus(status);
    }

    private String generateJobId(BatchJobRequest request) {
        return String.format("%s_%s_%s",
                request.getTemplateId(),
                request.getVaryingDimension().getParamName(),
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")));
    }

    private BatchJobStatus createInitialStatus(BatchJobRequest request) {
        return BatchJobStatus.builder()
                .jobId(request.getJobId())
                .templateId(request.getTemplateId())
                .variantCode(request.getVariantCode())
                .currentPhase(BatchJobStatus.JobPhase.SUBMITTED)
                .status(BatchJobStatus.JobState.PENDING)
                .progress(BatchJobStatus.ProgressInfo.builder()
                        .totalReports(request.getVaryingDimension().getValues().size())
                        .completedReports(0).failedReports(0).percentageComplete(0.0)
                        .completedValues(ConcurrentHashMap.newKeySet())
                        .failedValues(ConcurrentHashMap.newKeySet())
                        .build())
                .errors(BatchJobStatus.ErrorInfo.builder()
                        .totalErrors(0)
                        .recentErrors(new LinkedList<>())
                        .build())
                .build();
    }

    private void updatePhase(BatchJobStatus status, BatchJobStatus.JobPhase phase) {
        status.setCurrentPhase(phase);
        status.setLastUpdateTime(LocalDateTime.now());
        persistStatus(status);
        log.info("Job {} entering phase: {}", status.getJobId(), phase);
    }

    private void persistStatus(BatchJobStatus status) {
        batchJobStatusStore.save(status, Duration.ofMinutes(statusTtlMinutes));
    }

    private Path createHdfsDirectory(String basePath, LocalDate date,
                                     String afterDateFilePath) throws IOException {
        String dateStr = date.format(DateTimeFormatter.ISO_LOCAL_DATE);
        Path path = new Path(basePath, dateStr + "/" + afterDateFilePath);
        if (!hdfsFileSystem.exists(path)) {
            hdfsFileSystem.mkdirs(path);
        }
        return path;
    }

    private void logJobSummary(BatchJobStatus status) {
        log.info("===== JOB SUMMARY =====");
        log.info("Job ID: {}", status.getJobId());
        log.info("Total: {}", status.getProgress().getTotalReports());
        log.info("Completed: {}", status.getProgress().getCompletedReports());
        log.info("Failed: {}", status.getProgress().getFailedReports());
        if (status.getProgress().getTotalReports() > 0) {
            log.info("Success Rate: {}%",
                    String.format("%.1f", (status.getProgress().getCompletedReports() * 100.0)
                            / status.getProgress().getTotalReports()));
        }
        log.info("=======================");
    }

    private ReportTemplate cloneTemplate(ReportTemplate template) {
        try {
            return objectMapper.readValue(
                    objectMapper.writeValueAsBytes(template), ReportTemplate.class);
        } catch (IOException e) {
            log.error("Deep copy failed for template {}: {}",
                    template.getReportMeta().getReportId(), e.getMessage());
            throw new RuntimeException("Critical: Template cloning failed", e);
        }
    }

    // -------------------------------------------------------------------------
    // Inner record for analysis results (unchanged)
    // -------------------------------------------------------------------------

    private static class AnalysisResult {
        final ReportTemplate template;
        final ReportVariant variant;
        final List<DataSourceSpec> allSpecs;
        final List<QueryPattern> patterns;
        final boolean useBatchMode;
        final Map<String, Object> staticContext;
        final Map<String, Map<String, Object>> preFetchedGlobals;

        AnalysisResult(ReportTemplate template, ReportVariant variant,
                       List<DataSourceSpec> allSpecs, List<QueryPattern> patterns,
                       boolean useBatchMode, Map<String, Object> staticContext,
                       Map<String, Map<String, Object>> preFetchedGlobals) {
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

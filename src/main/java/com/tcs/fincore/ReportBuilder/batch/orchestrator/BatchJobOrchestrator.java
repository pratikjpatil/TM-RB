package com.tcs.fincore.ReportBuilder.batch.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.batch.application.model.BatchAnalysisResult;
import com.tcs.fincore.ReportBuilder.batch.application.port.BatchDataMaterializationCoordinator;
import com.tcs.fincore.ReportBuilder.batch.application.port.BatchReportGenerator;
import com.tcs.fincore.ReportBuilder.batch.application.port.BatchTemplateAnalyzer;
import com.tcs.fincore.ReportBuilder.batch.distributor.ResultDistributor;
import com.tcs.fincore.ReportBuilder.batch.status.BatchJobStatusStore;
import com.tcs.fincore.ReportBuilder.batch.store.BatchResultStore;
import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobStatus;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;
import com.tcs.fincore.ReportBuilder.kafka.publisher.ReportEventPublisher;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchJobOrchestrator {

    private final ObjectMapper objectMapper;
    private final BatchTemplateAnalyzer batchTemplateAnalysisService;
    private final BatchDataMaterializationCoordinator batchDataMaterializationService;
    private final BatchReportGenerator batchReportGenerationService;
    private final BatchJobStatusStore batchJobStatusStore;
    private final BatchResultStore batchResultStore;
    private final ResultDistributor resultDistributor;
    private final ReportEventPublisher eventPublisher;
    @Qualifier("batchReportGenerationExecutor")
    private final Executor reportGenerationExecutor;

    @Lazy
    @Autowired
    private BatchJobOrchestrator selfProxy;

    @Value("${report.batch.materialization.timeout-minutes:30}")
    private long materializationTimeoutMinutes;

    @Value("${report.batch.generation.timeout-minutes:240}")
    private long generationTimeoutMinutes;

    @Value("${report.batch.status.ttl-minutes:120}")
    private long statusTtlMinutes;

    @Value("${report.batch.status.persist-interval-ms:1000}")
    private long statusPersistIntervalMs;

    @Value("${report.batch.max-reports-per-job:50000}")
    private int maxReportsPerJob;

    private final ConcurrentHashMap<String, AtomicBoolean> cancellationTokens = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<Future<?>>> jobFutures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> lastStatusPersistTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ReportExecutionContext> executionContexts = new ConcurrentHashMap<>();

    @PostConstruct
    public void setup() {
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        objectMapper.configure(
                com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        log.info("BatchJobOrchestrator initialized. materializationTimeout={}m, generationTimeout={}m",
                materializationTimeoutMinutes, generationTimeoutMinutes);
    }

    @Scheduled(fixedDelayString = "${report.batch.status.cleanup-interval-ms:1800000}")
    public void evictStaleJobStatuses() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(statusTtlMinutes);
        int evicted = batchJobStatusStore.evictStaleTerminal(cutoff);
        if (evicted > 0) {
            log.info("Evicted {} stale job status entries (TTL={}m)", evicted, statusTtlMinutes);
        }
    }

    public BatchJobStatus submitJob(BatchJobRequest request) {
        return submitJob(request, ReportExecutionContext.fromRest());
    }

    public BatchJobStatus submitJob(BatchJobRequest request, ReportExecutionContext context) {
        if (request.getJobId() == null || request.getJobId().isBlank()) {
            request.setJobId(generateJobId(request));
        }

        validateRequest(request);

        BatchJobStatus status = createInitialStatus(request);
        persistStatus(status, true);

        cancellationTokens.put(request.getJobId(), new AtomicBoolean(false));
        jobFutures.put(request.getJobId(), Collections.synchronizedList(new ArrayList<>()));
        executionContexts.put(request.getJobId(), context);

        selfProxy.processJobAsync(request);
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
            persistStatus(status, true);
            log.info("Job {} cancelled", jobId);
        }
    }

    @Async("reportExecutor")
    public void processJobAsync(BatchJobRequest request) {
        String jobId = request.getJobId();
        BatchJobStatus status = batchJobStatusStore.find(jobId)
                .orElseThrow(() -> new ValidationException("Job not found: " + jobId));
        ensureThreadSafeStatusCollections(status);
        AtomicBoolean cancelled = cancellationTokens.get(jobId);
        ReportExecutionContext context = executionContexts.getOrDefault(jobId, ReportExecutionContext.fromRest());

        Instant jobStartInstant = Instant.now();

        try {
            log.info("=== BATCH JOB STARTED: {} (trigger={}) ===", jobId, context.getTriggerSource());
            status.setStatus(BatchJobStatus.JobState.RUNNING);
            status.setStartTime(LocalDateTime.now());
            persistStatus(status, true);

            eventPublisher.publishBatchStarted(context, status);

            updatePhase(status, BatchJobStatus.JobPhase.ANALYZING_TEMPLATE);
            BatchAnalysisResult analysis = batchTemplateAnalysisService.analyze(request);

            if (cancelled.get()) {
                markCancelled(status, jobId);
                return;
            }

            if (analysis.useBatchMode()) {
                updatePhase(status, BatchJobStatus.JobPhase.MATERIALIZING_DATA);
                batchDataMaterializationService.materialize(
                        jobId,
                        analysis,
                        status,
                        materializationTimeoutMinutes,
                        statusTtlMinutes,
                        this::persistStatus);
            } else {
                log.info("Batch mode not beneficial — using sequential per-report queries");
            }

            if (cancelled.get()) {
                markCancelled(status, jobId);
                return;
            }

            updatePhase(status, BatchJobStatus.JobPhase.GENERATING_REPORTS);
            batchReportGenerationService.generateReportsInBatches(
                    request,
                    analysis,
                    status,
                    cancelled,
                    toExecutorService(reportGenerationExecutor),
                    jobFutures.get(jobId),
                    generationTimeoutMinutes,
                    () -> markCancelled(status, jobId),
                    completed -> updateProgress(status, completed),
                    (dimensionValue, exception) -> {
                        recordError(status, dimensionValue, exception);
                        persistStatus(status);
                    },
                    () -> {
                        status.setStatus(BatchJobStatus.JobState.PARTIALLY_COMPLETED);
                        persistStatus(status, true);
                    });

            if (!cancelled.get()) {
                updatePhase(status, BatchJobStatus.JobPhase.COMPLETED);
                status.setStatus(BatchJobStatus.JobState.COMPLETED);
                status.setCompletionTime(LocalDateTime.now());
                persistStatus(status, true);
                log.info("=== BATCH JOB COMPLETED: {} ===", jobId);
                logJobSummary(status);
                eventPublisher.publishBatchTerminal(context, status, jobStartInstant);
            }

        } catch (RuntimeException e) {
            log.error("Batch job {} failed: {}", jobId, e.getMessage(), e);
            status.setCurrentPhase(BatchJobStatus.JobPhase.FAILED);
            status.setStatus(BatchJobStatus.JobState.FAILED);
            status.setCompletionTime(LocalDateTime.now());
            status.getErrors().setLastErrorMessage(e.getMessage());
            status.getErrors().setLastErrorTime(LocalDateTime.now());
            persistStatus(status, true);

            eventPublisher.publishBatchTerminal(context, status, jobStartInstant);

        } finally {
            batchResultStore.cleanupJob(jobId);
            resultDistributor.clearJobContext(jobId);
            cancellationTokens.remove(jobId);
            jobFutures.remove(jobId);
            executionContexts.remove(jobId);
            lastStatusPersistTimes.remove(jobId);
        }
    }

    private void updateProgress(BatchJobStatus status, int completed) {
        synchronized (status.getProgress()) {
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
        }
        status.setLastUpdateTime(LocalDateTime.now());
        persistStatus(status);
    }

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
        if (maxReports > maxReportsPerJob) {
            throw new ValidationException(
                    "Too many dimension values (" + maxReports + ", max: " + maxReportsPerJob + ")");
        }
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
        persistStatus(status, true);
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
        persistStatus(status, true);
        log.info("Job {} entering phase: {}", status.getJobId(), phase);
    }

    private void persistStatus(BatchJobStatus status) {
        persistStatus(status, false);
    }

    private void persistStatus(BatchJobStatus status, boolean force) {
        String jobId = status.getJobId();
        long now = System.currentTimeMillis();
        AtomicLong lastPersist = lastStatusPersistTimes.computeIfAbsent(jobId, key -> new AtomicLong(0));
        if (!force && statusPersistIntervalMs > 0) {
            long previous = lastPersist.get();
            if (now - previous < statusPersistIntervalMs) {
                return;
            }
        }
        batchJobStatusStore.save(status, Duration.ofMinutes(statusTtlMinutes));
        lastPersist.set(now);
    }

    private void ensureThreadSafeStatusCollections(BatchJobStatus status) {
        if (status == null || status.getProgress() == null) {
            return;
        }
        BatchJobStatus.ProgressInfo progress = status.getProgress();
        synchronized (progress) {
            Set<String> completedValues = progress.getCompletedValues();
            if (completedValues == null) {
                progress.setCompletedValues(ConcurrentHashMap.newKeySet());
            } else if (!(completedValues instanceof ConcurrentHashMap.KeySetView)) {
                Set<String> threadSafeCompleted = ConcurrentHashMap.newKeySet();
                threadSafeCompleted.addAll(completedValues);
                progress.setCompletedValues(threadSafeCompleted);
            }

            Set<String> failedValues = progress.getFailedValues();
            if (failedValues == null) {
                progress.setFailedValues(ConcurrentHashMap.newKeySet());
            } else if (!(failedValues instanceof ConcurrentHashMap.KeySetView)) {
                Set<String> threadSafeFailed = ConcurrentHashMap.newKeySet();
                threadSafeFailed.addAll(failedValues);
                progress.setFailedValues(threadSafeFailed);
            }
        }
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

    private ExecutorService toExecutorService(Executor executor) {
        if (executor instanceof ExecutorService executorService) {
            return executorService;
        }
        throw new IllegalStateException("batchReportGenerationExecutor must be an ExecutorService implementation");
    }
}

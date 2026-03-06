package com.tcs.fincore.ReportBuilder.batch.application.service;

import com.tcs.fincore.ReportBuilder.batch.distributor.ResultDistributor;
import com.tcs.fincore.ReportBuilder.batch.application.model.BatchAnalysisResult;
import com.tcs.fincore.ReportBuilder.batch.application.port.BatchReportGenerator;
import com.tcs.fincore.ReportBuilder.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.dto.BatchJobStatus;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.service.aggregation.AggregatorService;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import com.tcs.fincore.ReportBuilder.service.calculation.CalculationService;
import com.tcs.fincore.ReportBuilder.service.runtime.ReportRuntimeService;
import com.tcs.fincore.ReportBuilder.service.template.TemplateProcessorService;
import com.tcs.fincore.ReportBuilder.batch.application.util.TemplateCloner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchReportGenerationService implements BatchReportGenerator {

    private final ReportRuntimeService runtimeService;
    private final TemplateProcessorService templateProcessor;
    private final ResultDistributor resultDistributor;
    private final AggregatorService aggregatorService;
    private final CalculationService calculationService;
    private final TemplateCloner templateCloner;
    private final BatchReportStorageService reportStorageService;

    public void generateReportsInBatches(BatchJobRequest request,
                                         BatchAnalysisResult analysis,
                                         BatchJobStatus status,
                                         AtomicBoolean cancelled,
                                         ExecutorService reportGenerationExecutor,
                                         List<Future<?>> futures,
                                         long generationTimeoutMinutes,
                                         Runnable onCancelled,
                                         Consumer<Integer> onProgress,
                                         BiConsumer<String, Exception> onError,
                                         Runnable onPartialCompletion) {
        List<String> allValues = request.getVaryingDimension().getValues();
        int totalReports = allValues.size();
        status.getProgress().setTotalReports(totalReports);
        status.setBatchProcessStartTime(LocalDateTime.now());

        List<List<String>> batches = splitIntoBatches(allValues, request.getBatchSize());
        log.info("Phase 3: Generating {} reports in {} batches", totalReports, batches.size());

        AtomicInteger completedCount = new AtomicInteger(0);

        for (List<String> batch : batches) {
            if (cancelled.get()) {
                onCancelled.run();
                return;
            }

            List<CompletableFuture<Void>> batchFutures = new ArrayList<>();

            for (String dimensionValue : batch) {
                if (cancelled.get()) {
                    break;
                }

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        generateSingleReport(request, analysis, dimensionValue);
                        int done;
                        synchronized (status.getProgress()) {
                            status.getProgress().getCompletedValues().add(dimensionValue);
                            done = completedCount.incrementAndGet();
                        }
                        onProgress.accept(done);
                    } catch (Exception e) {
                        log.error("Failed to generate report for {}: {}", dimensionValue, e.getMessage(), e);
                        synchronized (status.getProgress()) {
                            status.getProgress().getFailedValues().add(dimensionValue);
                            status.getProgress().setFailedReports(status.getProgress().getFailedReports() + 1);
                        }
                        onError.accept(dimensionValue, e);
                    }
                }, reportGenerationExecutor);

                batchFutures.add(future);
                futures.add(future);
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

        int failed = status.getProgress().getFailedReports();
        int completed = completedCount.get();
        if (failed > 0 && completed > 0) {
            onPartialCompletion.run();
        }
    }

    private void generateSingleReport(BatchJobRequest request,
                                      BatchAnalysisResult analysis,
                                      String dimensionValue) throws Exception {
        Map<String, Object> runInputs = new HashMap<>(analysis.staticContext());
        runInputs.put(request.getVaryingDimension().getParamName(), dimensionValue);

        if (analysis.preFetchedGlobals() != null) {
            analysis.preFetchedGlobals().forEach((paramName, valueMap) -> {
                if (valueMap.containsKey(dimensionValue)) {
                    runInputs.put(paramName, valueMap.get(dimensionValue));
                }
            });
        }

        ReportTemplate templateCopy = templateCloner.deepClone(analysis.template());
        runtimeService.prepareRuntimeContext(templateCopy, analysis.variant(), runInputs);
        List<DataSourceSpec> currentSpecs = templateProcessor.extractDbSpecs(templateCopy);

        Map<String, Object> dataResults;
        if (analysis.useBatchMode()) {
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

        reportStorageService.saveReports(
                request.getHdfsBasePath(),
                request.getCommonParams(),
                request.getVaryingDimension().getParamName(),
                dimensionValue,
                request.getOutputFormats(),
                analysis.template(),
                analysis.variant(),
                exportModel);
    }

    private List<List<String>> splitIntoBatches(List<String> items, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < items.size(); i += batchSize) {
            batches.add(new ArrayList<>(items.subList(i, Math.min(items.size(), i + batchSize))));
        }
        return batches;
    }
}

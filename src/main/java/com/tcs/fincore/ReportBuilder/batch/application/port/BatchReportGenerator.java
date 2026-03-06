package com.tcs.fincore.ReportBuilder.batch.application.port;

import com.tcs.fincore.ReportBuilder.batch.application.model.BatchAnalysisResult;
import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobStatus;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface BatchReportGenerator {
    void generateReportsInBatches(BatchJobRequest request,
                                  BatchAnalysisResult analysis,
                                  BatchJobStatus status,
                                  AtomicBoolean cancelled,
                                  ExecutorService reportGenerationExecutor,
                                  List<Future<?>> futures,
                                  long generationTimeoutMinutes,
                                  Runnable onCancelled,
                                  Consumer<Integer> onProgress,
                                  BiConsumer<String, Exception> onError,
                                  Runnable onPartialCompletion);
}

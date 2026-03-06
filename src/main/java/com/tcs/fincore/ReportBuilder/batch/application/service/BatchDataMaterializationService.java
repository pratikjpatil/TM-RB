package com.tcs.fincore.ReportBuilder.batch.application.service;

import com.tcs.fincore.ReportBuilder.batch.distributor.ResultDistributor;
import com.tcs.fincore.ReportBuilder.batch.materializer.BatchDataMaterializer;
import com.tcs.fincore.ReportBuilder.batch.application.model.BatchAnalysisResult;
import com.tcs.fincore.ReportBuilder.batch.application.port.BatchDataMaterializationCoordinator;
import com.tcs.fincore.ReportBuilder.batch.store.BatchResultStore;
import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchDataMaterializationService implements BatchDataMaterializationCoordinator {

    private final BatchDataMaterializer dataMaterializer;
    private final BatchResultStore batchResultStore;
    private final ResultDistributor resultDistributor;

    public void materialize(String jobId,
                            BatchAnalysisResult analysis,
                            BatchJobStatus status,
                            long materializationTimeoutMinutes,
                            long statusTtlMinutes,
                            Consumer<BatchJobStatus> statusPersister) {

        log.info("Phase 2: Materializing {} patterns (timeout={}m)",
                analysis.patterns().size(), materializationTimeoutMinutes);
        long startTime = System.currentTimeMillis();

        if (status != null) {
            status.getProgress().setMaterializedRows(0L);
            status.setLastUpdateTime(LocalDateTime.now());
            statusPersister.accept(status);
        }

        batchResultStore.initializeJob(jobId, Duration.ofMinutes(statusTtlMinutes));
        AtomicLong pendingRows = new AtomicLong(0);
        dataMaterializer.materializeAll(jobId, analysis.patterns(), materializationTimeoutMinutes, rows -> {
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
                statusPersister.accept(status);
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
            statusPersister.accept(status);
        }

        resultDistributor.initialize(
                jobId, analysis.patterns(),
                analysis.preFetchedGlobals(), analysis.template());

        log.info("Phase 2 complete in {}ms: {} patterns materialized",
                System.currentTimeMillis() - startTime, analysis.patterns().size());
    }
}

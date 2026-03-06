package com.tcs.fincore.ReportBuilder.batch.application.port;

import com.tcs.fincore.ReportBuilder.batch.application.model.BatchAnalysisResult;
import com.tcs.fincore.ReportBuilder.dto.BatchJobStatus;

import java.util.function.Consumer;

public interface BatchDataMaterializationCoordinator {
    void materialize(String jobId,
                     BatchAnalysisResult analysis,
                     BatchJobStatus status,
                     long materializationTimeoutMinutes,
                     long statusTtlMinutes,
                     Consumer<BatchJobStatus> statusPersister);
}

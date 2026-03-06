package com.tcs.fincore.ReportBuilder.batch.application.port;

import com.tcs.fincore.ReportBuilder.batch.application.model.BatchAnalysisResult;
import com.tcs.fincore.ReportBuilder.dto.BatchJobRequest;

public interface BatchTemplateAnalyzer {
    BatchAnalysisResult analyze(BatchJobRequest request);
}
